import argparse
import multiprocessing
import logging
import json
import os
import queue
import sys
import exiftool             # Requires pip3 install pyexiftool >= 0.5
import datetime
import checksum_mgr
import random

def _parse_args():
    arg_parser = argparse.ArgumentParser(description="End of day script to create validated travel copies of all pics")

    arg_parser.add_argument("--debug", help="Lower logging level from INFO to DEBUG", action="store_true")

    # The "Action append" lets the parameter be specified multiple times, but it's still optional so may exist no times
    arg_parser.add_argument("--sourcedir",
                            help="Path to a source directory with RAW files",
                            required=True,
                            action='append')

    arg_parser.add_argument("--timestamp_utc_offset_hours",
                            help="Integer hours offset from UTC, e.g., EDT is -4, PDT is -7",
                            type=int,
                            default=0)

    #num_checksum_processes_default = multiprocessing.cpu_count() - 1
    num_checksum_processes_default = 6      # Determined experimentally
    arg_parser.add_argument("--checksum_processes",
                            help="Number of checksum processes to launch" +
                                f" (default on this computer: {num_checksum_processes_default})",
                            default=num_checksum_processes_default,
                            type=int )

    checksum_queue_length_default = 9500            # Determined experimentally
    arg_parser.add_argument("--checksum_queue_length",
                            help="Length of the checksum queue " +
                                 f" (default on this computer: {checksum_queue_length_default})",
                            default=checksum_queue_length_default,
                            type=int)

    known_raw_file_extensions = ['NEF', 'CR3']
    arg_parser.add_argument("raw_file_fileext",
                            type=str.upper,
                            choices=known_raw_file_extensions,
                            help=f"File extension for RAW files" )

    arg_parser.add_argument("travel_storage_media_folder", nargs="+",
                            help="Travel storage folder " + \
                                 "(e.g., laptop NVMe drive, SanDisk Extreme Pro 4TB, WD My Passport 4TB)")

    return arg_parser.parse_args()


def _scan_source_dir_for_images( curr_source_dir, program_options ):
    image_files = []
    cumulative_bytes = 0
    print(f"\tFinding \".{program_options['file_extension']}\" files in sourcedir \"{curr_source_dir}\"")

    for subdir, dirs, files in os.walk(curr_source_dir):
        for filename in files:
            if filename.lower().endswith(program_options['file_extension']):
                file_absolute_path = os.path.join(curr_source_dir, subdir, filename)
                #print( "\tFound image with full path: \"{0}\"".format(file_absolute_path))
                file_size_bytes = os.path.getsize(file_absolute_path)
                # logging.debug(f"File size of {file_absolute_path}: {file_size_bytes}")
                cumulative_bytes += file_size_bytes
                image_files.append(
                    {
                        'file_path': {
                            'absolute': file_absolute_path,
                            'relative': os.path.relpath(file_absolute_path, curr_source_dir)
                        },
                        'filesize_bytes': file_size_bytes,
                    }
                )
                # break
            else:
                # logging.debug( "Skipping non-image file {0}".format(filename))
                pass

    # Walk complete, send results to parent
    return image_files

def _enumerate_source_images(program_options):
    print( "\nEnumerating source images")

    source_file_lists = {}

    # Create a dictionary that maps all absolute file paths to the relative one they have in common
    for curr_sourcedir in program_options['sourcedirs']:
        source_file_lists[curr_sourcedir] = _scan_source_dir_for_images(curr_sourcedir, program_options )
        print( f"\tFound {len(source_file_lists[curr_sourcedir])} \".{program_options['file_extension']}\" files" )

    return source_file_lists


def _extract_exif_timestamps( program_options, source_file_lists ):
    print("\nExtracting EXIF timestamps from all source images")

    # Populate EXIF timestamps for all images
    raw_file_list = []
    reverse_map = {}
    for curr_sourcedir in source_file_lists:
        for (index, curr_file_entry) in enumerate(source_file_lists[curr_sourcedir]):
            raw_file_list.append(curr_file_entry['file_path']['absolute'])

            # Store a reference into the source array keyed by absolute filename so we can do an O(1) lookup
            #   when we need to update with the timestamp
            reverse_map[ curr_file_entry['file_path']['absolute'] ] = source_file_lists[curr_sourcedir][index]

    #print( f"Reverse map created:\n{json.dumps(reverse_map, indent=4, sort_keys=True)}")

    # Randomly shuffle the file list so that we send our queries across a fairly even distribution of sourcedirs
    #   e.g., don't send all drive 1 entries then drive 2 entries, this keeps the load even over all sourcedirs
    random.shuffle(raw_file_list)

    # Two queues, one to send files to process to workers, one for workers to send us timestamps back
    files_to_timestamp_queue = multiprocessing.Queue()
    timestamped_files_queue = multiprocessing.Queue()

    # Event to tell everyone when all the work is done
    all_hashes_read = multiprocessing.Event()

    num_exiftool_workers = multiprocessing.cpu_count() - 1
    exiftool_worker_handles = []
    for i in range( num_exiftool_workers ):
        worker_name = f"exiftool_worker_{i+1}"
        curr_handle = multiprocessing.Process(target=_exiftool_worker,
                                              args=(worker_name, files_to_timestamp_queue, timestamped_files_queue,
                                                    all_hashes_read, program_options['timestamp_utc_offset_hours']) )
        exiftool_worker_handles.append(curr_handle)
        curr_handle.start()

    timestamps_received = 0
    timestamps_expected = len(raw_file_list)
    curr_file_index_to_send = 0

    while timestamps_received < timestamps_expected:
        # Send a file to timestamp
        if curr_file_index_to_send < timestamps_expected:
            files_to_timestamp_queue.put( raw_file_list[curr_file_index_to_send] )
            curr_file_index_to_send += 1

        # Read extracted timestamps until queue is empty
        try:
            extracted_timestamp_info = timestamped_files_queue.get_nowait()
            timestamps_received += 1

            reverse_map[ extracted_timestamp_info['absolute_path'] ]['timestamp'] = extracted_timestamp_info['timestamp']

        except queue.Empty:
            # No worries, just loop back and try it all again
            pass

    # Let the children know we're all done
    all_hashes_read.set()

    # Land all the worker processes and clean up their resources
    while exiftool_worker_handles:
        curr_handle = exiftool_worker_handles.pop()
        curr_handle.join()
        curr_handle.close()

    # Debug print one entry to make sure timestamp looks sane
    #print( json.dumps( reverse_map[ raw_file_list[0] ], indent=4, sort_keys=True, default=str) )

    print( f"\tParsed EXIF timestamps from {timestamps_expected} \".{program_options['file_extension']}\" files")


def _exiftool_worker( worker_name, files_to_timestamp_queue, timestamped_files_queue, all_hashes_read,
                      timestamp_utc_offset_hours ):

    exiftool_tag_name = "EXIF:DateTimeOriginal"
    with exiftool.ExifToolHelper() as exiftool_handle:
        while all_hashes_read.is_set() is False:
            try:
                file_to_timestamp = files_to_timestamp_queue.get(timeout=0.1)
            except queue.Empty:
                continue

            file_timestamps = exiftool_handle.get_tags( file_to_timestamp, tags = exiftool_tag_name )
            curr_timestamp_entry = file_timestamps[0]

            file_datetime_no_tz = datetime.datetime.strptime(curr_timestamp_entry[exiftool_tag_name], "%Y:%m:%d %H:%M:%S")
            # Do hour shift from timezone-unaware EXIF datetime to UTC (still no TZ, just shifting hours)
            shifted_datetime_no_tz = file_datetime_no_tz + datetime.timedelta(
                hours=-timestamp_utc_offset_hours )
            # Create TZ-aware datetime, as one should basically always strive to use
            file_datetime_utc = shifted_datetime_no_tz.replace(tzinfo=datetime.timezone.utc)

            # Send this info back home
            extracted_timestamp = {
                'absolute_path' : file_to_timestamp,
                'timestamp'     : file_datetime_utc,
            }

            timestamped_files_queue.put( extracted_timestamp )

def _validate_sourcedir_lists_match( program_options, source_file_lists ):

    print("\nValidating all sourcedir file lists are identical")

    # Make sure all file lists came out identical
    for curr_sourcedir in program_options['sourcedirs'][2:]:
        if source_file_lists[program_options['sourcedirs'][0]] != source_file_lists[curr_sourcedir]:
            logging.error( "Contents of source dirs do not match, bailing")
            sys.exit( 1 )

    print( "\tMetadata contents of all sourcedirs match!")

    total_file_count = len( source_file_lists[program_options['sourcedirs'][0]] )

    #total_file_count = 9876

    # Create return list (only need relative paths)
    file_dict = {}
    total_file_bytes = 0
    for curr_file_entry in source_file_lists[program_options['sourcedirs'][0]]:
        file_dict[ curr_file_entry['file_path']['relative']] = {
            'filesize_bytes'    : curr_file_entry['filesize_bytes'],
            'timestamp'         : curr_file_entry['timestamp']
        }
        total_file_bytes += curr_file_entry['filesize_bytes']

    bytes_in_gb = 1024.0 * 1024.0 * 1024.0
    total_file_gb = total_file_bytes / bytes_in_gb

    print ( f"\tFound {total_file_count:,} \".{program_options['file_extension']}\" files totalling " +
            f"{total_file_gb:.02f} GB")

    return {
        # Only need to return one source file list, as they're all identical
        'source_file_dict'  : file_dict,
        'total_file_count'  : total_file_count,
        'total_file_bytes'  : total_file_bytes
    }


def _set_destination_filenames( program_options, source_image_info ):
    print("\nDetermining unique filenames in destination storage directories")
    destination_dir_prefix = program_options['destination_folders'][0]

    # Resolve filename conflicts in the YYYY/YYYY-MM-DD destination dir
    filename_conflicts_found = 0
    for source_filename in source_image_info:
        logging.debug( f"Determining unique destination filename for {source_filename}")
        year = source_image_info[source_filename]['timestamp'].year
        yearmonthday = source_image_info[source_filename]['timestamp'].isoformat()[:10]

        #print( f"Destination path: {destination_path}")
        source_image_info[source_filename]['destination'] = {
            'relative_directory'    : os.path.join( str(year), yearmonthday ),
        }

        # Check to see if relative path is clear
        destination_path = os.path.join(destination_dir_prefix, str(year), yearmonthday, source_filename)

        unique_extension = 1
        if os.path.exists(destination_path):
            filename_conflicts_found += 1
            while os.path.exists(destination_path):
                (filename_no_extension, filename_extension) = os.path.splitext( source_filename)
                destination_path = os.path.join(destination_dir_prefix, str(year), yearmonthday,
                                                f"{filename_no_extension}_{unique_extension:04d}{filename_extension}")
                unique_extension += 1
            source_image_info[source_filename]['destination']['unique_relative_destination_path'] = \
                os.path.join(str(year), yearmonthday,
                             f"{filename_no_extension}_{(unique_extension - 1):04d}{filename_extension}")
        else:
            source_image_info[source_filename]['destination']['unique_relative_destination_path'] = \
                os.path.join( str(year), yearmonthday, source_filename )

    #print( f"Updated source file info:\n{json.dumps(source_image_info, indent=4, sort_keys=True, default=str)}")

    num_files = len(source_image_info)
    #num_files = 4567
    print( f"\t{num_files:6,} \".{program_options['file_extension']}\" file(s) have had their unique destination paths determined")
    print( f"\t{filename_conflicts_found:6,} \".{program_options['file_extension']}\" file(s) had to have their destination paths updated due to existing files in the destination dir")


def _create_destination_writer_queues( program_options ):
    destination_writer_queues = []
    for curr_destination_folder in range(len(program_options['destination_folders'])):
        destination_writer_queues.append( multiprocessing.SimpleQueue() )

    return destination_writer_queues


def _write_to_destination_folder_worker( pipe_read_connection, destination_folder,
                                         number_of_sourcedirs,
                                         display_console_messages_queue,
                                         checksum_update_queue):

    worker_starting_msg = {
        "msg_level"         : logging.DEBUG,
        "msg"               : f"Worker process to write data to \"{destination_folder}\" has started",
    }

    display_console_messages_queue.put( worker_starting_msg )

    # Keep track of what files this destination writer has decided to write to disk
    files_being_written_to_disk = {}

    # Read out of pipe until it's closed
    sourcedirs_still_writing = number_of_sourcedirs
    while ( sourcedirs_still_writing > 0 ):
        data_received = pipe_read_connection.get()

        # See if it's a "done writing" message
        if data_received['message_type'] == "DONE_WRITING":
            sourcedirs_still_writing -= 1
            ending_write_msg = {
                "msg_level"     : logging.DEBUG,
                "msg"           : f"Destination writer for {destination_folder} got DONE_WRITING msg " + \
                    f"from sourcedir {data_received['sourcedir']}"
            }
            display_console_messages_queue.put(ending_write_msg)
        elif data_received['message_type'] == "DATA_BLOCK":


            #formulated_path = os.path.join( destination_folder, data_received['relative_path'])
            relative_path = data_received['file_info']['relative_path']
            relative_dir = data_received['file_info']['relative_dir']
            sourcedir = data_received['file_info']['sourcedir']
            byte_start = data_received['data_block']['block_byte_start']
            payload_len = data_received['data_block']['block_num_bytes']
            byte_end = data_received['data_block']['block_byte_end']
            total_file_size = data_received['file_info']['total_file_size']
            file_payload = data_received['data_block']['block_payload']

            # Let's see if we're going to write this update to disk or just ignore it
            if relative_path not in files_being_written_to_disk:
                # We're definitely going to write this bad boy to disk
                write_block_to_disk = True

                # Record the relevant info so we know if it's a duplicate or not
                files_being_written_to_disk[ relative_path ] = {
                    'sourcedir' : sourcedir
                }
            # As long as the sourcedir matches the one we're writing, we'll act on the write request
            elif files_being_written_to_disk[relative_path]['sourcedir'] == sourcedir:
                write_block_to_disk = True

            # This write came from a different sourcedir, so just politely but firmly discard the request
            else:
                write_block_to_disk = False

            if write_block_to_disk:

                #print( f"\n *** Destination absolute path created by joining folder {destination_folder} and path {relative_path}")

                destination_absolute_path = os.path.join(destination_folder, relative_path)

                # Is this the first data block for the file?
                if byte_start == 1:
                    # Create missing directories along the path, if any
                    destination_dir_absolute_path = os.path.join( destination_folder, relative_dir)
                    if os.path.isdir( destination_dir_absolute_path ) is False:
                        os.makedirs( destination_dir_absolute_path )

                    # Open for write (create) in binary mode
                    open_flags = "wb"

                    # Sanity check because some weird shit is happening
                    assert  os.path.isfile(destination_absolute_path) is False, \
                            f"Going to try to write to {destination_dir_absolute_path} for the first time, but the file existed"
                else:
                    # Open for append in binary mode
                    open_flags = "ab"

                    assert os.path.isfile(destination_absolute_path), \
                            f"Was going to append to {destination_dir_absolute_path} but the file doesn't exist"

                try:
                    with open( destination_absolute_path, open_flags ) as file_handle:
                        file_handle.write( file_payload )
                except:
                    print( f"Error hit when opening/writing {destination_absolute_path} with flags {open_flags}")
                    break

                # display_message = f"Destination writer for {destination_folder} got data block from {sourcedir} for {relative_path}, starting at byte {byte_start}, len={payload_len}, ending byte={byte_end}, total file size={total_file_size}"
                # writing_data_msg = {
                #     "msg_level"     : logging.DEBUG,
                #     "msg"           : display_message,
                # }
                # display_console_messages_queue.put( writing_data_msg)

                # If this is the last block we wrote out, note that the file we just finished writing needs
                #       checksumming
                if byte_end == total_file_size:
                    checksum_msg = {
                        'file_info': {
                            'absolute_path' : destination_absolute_path,
                            'relative_path' : relative_path,
                        }
                    }

                    checksum_update_queue.put( checksum_msg)

            file_payload = None
            del data_received['data_block']['block_payload']

        data_received = None

    worker_exiting_msg = {
        "msg_level"     : logging.DEBUG,
        "msg"           : f"Destination writer for {destination_folder} exiting cleanly"
    }

    display_console_messages_queue.put(worker_exiting_msg)


def _launch_destination_writers(program_options, display_console_messages_queue, checksum_update_queue ):
    destination_writer_queues = _create_destination_writer_queues(program_options)

    writer_process_handles = []

    number_of_sourcedirs = len(program_options['sourcedirs'])

    # Create processes to write to each destination
    for (i, curr_dest_folder) in enumerate(program_options['destination_folders']):
        curr_handle = multiprocessing.Process(target=_write_to_destination_folder_worker,
                                              args=( destination_writer_queues[i],
                                                     curr_dest_folder,
                                                     number_of_sourcedirs,
                                                     display_console_messages_queue,
                                                     checksum_update_queue) )

        writer_process_handles.append( curr_handle )
        curr_handle.start()

    return_dict = {
        "writer_queues"             : destination_writer_queues,
        "writer_process_handles"    : writer_process_handles,
    }

    return return_dict


def _read_from_sourcedir_worker( curr_sourcedir, source_image_info,
                                 display_console_messages_queue, destination_writer_queues,
                                 checksum_update_queue):

    worker_starting_msg = {
        "msg_level"         : logging.DEBUG,
        "msg"               : f"Worker process to read data from sourcedir \"{curr_sourcedir}\" has started",
    }

    display_console_messages_queue.put( worker_starting_msg )

    # Max block size = 1MB
    max_block_size = 1024 * 1024

    for curr_input_file in source_image_info:
        curr_input_file_data = source_image_info[curr_input_file]
        #print( "Input file:\n" + json.dumps(curr_input_file_data, indent=4, sort_keys=True, default=str))

        # Loop over block-sized chunks in the file
        curr_file_offset = 0
        sourcefile_absolute_path = os.path.join( curr_sourcedir, curr_input_file )
        curr_file_size = os.path.getsize(sourcefile_absolute_path)
        #print( f"Sourcefile {sourcefile_absolute_path} has file size of {curr_file_size} bytes")
        with open( sourcefile_absolute_path, "rb") as file_handle:
            while curr_file_offset + 1 < curr_file_size:
                bytes_for_block = min( curr_file_size - curr_file_offset, max_block_size )
                #print( f"Bytes for block: {bytes_for_block}")

                data_block_payload_bytes = file_handle.read(bytes_for_block)

                # Write the data block for checksumming
                checksum_update_request_msg = {
                    'file_info'     : {
                        'absolute_path'     : sourcefile_absolute_path,
                        'relative_path'     : curr_input_file_data['destination']['unique_relative_destination_path'],
                        'total_file_size'   : curr_file_size,
                    },
                    'data_block'    : {
                        "block_byte_start"  : curr_file_offset + 1,
                        "block_num_bytes"   : bytes_for_block,
                        "block_byte_end"    : curr_file_offset + bytes_for_block,
                        "block_payload"     : data_block_payload_bytes,
                    },
                }

                checksum_update_queue.put( checksum_update_request_msg )

                data_block_msg = {
                    'message_type'  : "DATA_BLOCK",
                    'file_info'     : {
                        'sourcedir'         : curr_sourcedir,
                        'relative_path'     : curr_input_file_data['destination']['unique_relative_destination_path'],
                        'total_file_size'   : curr_file_size,
                        'relative_dir'      : curr_input_file_data['destination']['relative_directory'],
                    },
                    "data_block": {
                        "block_byte_start"  : curr_file_offset + 1,
                        "block_num_bytes"   : bytes_for_block,
                        "block_byte_end"    : curr_file_offset + bytes_for_block,
                        "block_payload"     : data_block_payload_bytes,
                    }
                }

                # Write the block to all destinations
                for curr_writer_queue in destination_writer_queues:
                    curr_writer_queue.put(data_block_msg)

                # Drop the bytes as quickly as we can
                data_block_payload_bytes = None
                del data_block_msg['data_block']['block_payload']
                data_block_msg = None

                #print( "Wrote following block to all destinations:\n" + json.dumps(data_block_msg, indent=4, sort_keys=True, default=str))

                curr_file_offset += bytes_for_block

    done_writing_msg = {
        "sourcedir"     : curr_sourcedir,
        "message_type"  : "DONE_WRITING",
    }

    for curr_writer_queue in destination_writer_queues:
        curr_writer_queue.put( done_writing_msg )

    worker_terminating_msg = {
        "msg_level"         : logging.DEBUG,
        "msg"               : f"Worker process to read data from sourcedir \"{curr_sourcedir}\" exiting cleanly",
    }

    display_console_messages_queue.put( worker_terminating_msg )


def _launch_sourcedir_readers( program_options, source_image_info, display_console_messages_queue,
                               destination_writers,
                               checksum_update_queue ):

    reader_process_handles = []
    #print( f"Destination writers:\n{json.dumps(destination_writers, indent=4, sort_keys=True, default=str)}")

    for ( i, curr_sourcedir ) in enumerate( program_options['sourcedirs']):
        curr_handle = multiprocessing.Process( target=_read_from_sourcedir_worker,
                                               args=(curr_sourcedir, source_image_info['source_file_dict'],
                                                     display_console_messages_queue,
                                                     destination_writers['writer_queues'],
                                                     checksum_update_queue) )
        reader_process_handles.append( curr_handle )
        curr_handle.start()

    return reader_process_handles

def _main():
    args = _parse_args()
    program_options = {}

    if args.debug is False:
        log_level = logging.INFO
    else:
        log_level = logging.DEBUG

    logging.basicConfig(level=log_level)

    if args.sourcedir:
        program_options['sourcedirs'] = sorted(args.sourcedir)
    else:
        program_options['sourcedirs'] = []

    program_options['file_extension'] = args.raw_file_fileext.lower()
    program_options['timestamp_utc_offset_hours'] = args.timestamp_utc_offset_hours
    program_options['destination_folders'] = args.travel_storage_media_folder
    program_options['checksum_processes'] = args.checksum_processes
    program_options['checksum_queue_length'] = args.checksum_queue_length

    logging.debug( f"Program options: {json.dumps(program_options, indent=4, sort_keys=True)}" )

    source_file_lists = _enumerate_source_images(program_options)
    #print( "\nSource file info:\n" + json.dumps(source_image_info['source_file_dict'],
    #                                                             indent=4, sort_keys=True, default=str) )

    _extract_exif_timestamps( program_options, source_file_lists )

    source_image_info = _validate_sourcedir_lists_match(program_options, source_file_lists )

    # Determine unique filenames
    _set_destination_filenames( program_options, source_image_info['source_file_dict'] )

    # Create queue that all children use to send messages for display back up to parent
    display_console_messages_queue = multiprocessing.Queue()

    checksum_manager = checksum_mgr.ChecksumManager(len(source_image_info['source_file_dict']) *
                                                    (len(program_options['sourcedirs']) +
                                                     len(program_options['destination_folders'])),
                                                    display_console_messages_queue,
                                                    program_options['checksum_queue_length'],
                                                    program_options['checksum_processes'])

    # Launch destination writers
    destination_writers = _launch_destination_writers( program_options, display_console_messages_queue,
                                                       checksum_manager.checksum_update_queue)

    # Launch sourcedir readers
    sourcedir_readers = _launch_sourcedir_readers( program_options, source_image_info,
                                                   display_console_messages_queue,
                                                   destination_writers,
                                                   checksum_manager.checksum_update_queue)

    print( "\nStarting combination file copy/checksum computation operations")

    # Read out all messages from the display queue
    blocking_read = True
    read_timeout_seconds = 0.05
    try:
        #print( "\n\n")
        while True:
            process_started_msg = display_console_messages_queue.get(blocking_read, read_timeout_seconds)

            # TODO: maybe move this to its own process and do something useful, but for now, politely ignore
            #print( f"Parent got message to display: \"{process_started_msg['msg']}\"")
    except queue.Empty:
        pass

    # Now let's wait for checksum data to come in
    number_unique_files = len(source_image_info['source_file_dict'])
    number_copies_of_unique_files = len(program_options['sourcedirs']) + len(program_options['destination_folders'])
    checksum_manager.validate_all_checksums_match( number_unique_files, number_copies_of_unique_files )

    # Now clean up the reader/writer processes

    while len(sourcedir_readers) > 0:
        curr_reader_handle = sourcedir_readers.pop()
        #print( "Parent waiting for sourcedir reader child process to rejoin")
        curr_reader_handle.join()
        #print( "Parent had sourcedir reader child process rejoin")

    while len(destination_writers['writer_process_handles']) > 0:
        curr_handle = destination_writers['writer_process_handles'].pop()
        #print( "Parent waiting for destination writer child process to rejoin" )
        curr_handle.join()
        #print( "Parent had destination writer child process rejoin")

    #print( "parent terminating cleanly" )

if __name__ == "__main__":
    _main()