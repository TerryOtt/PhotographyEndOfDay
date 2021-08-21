import pprint
import json
import logging
import argparse
import time
import os
import hashlib
import multiprocessing
import queue
import exiftool
import datetime


def _parse_args():
    arg_parser = argparse.ArgumentParser(description="End of day script to validate data, geotag, copy to SSD")
    arg_parser.add_argument("--debug", help="Lower logging level from INFO to DEBUG", action="store_true" )
    arg_parser.add_argument("--logfile", help="Path to file where the app log will be created",
                            default="photo_end_of_day.log" )
    arg_parser.add_argument( "--sourcedir", help="Path to a source directory", required=True, action="append" )
    arg_parser.add_argument( "fileext", help="File extension, e.g. \"NEF\", \"CR3\""  )
    arg_parser.add_argument( "exiftool", help="Path to the exiftool utility" )
    arg_parser.add_argument("file_timestamp_utc_offset_hours",
                            help="Hours offset from UTC, e.g. EDT is -4, Afghanistan is +4.5",
                            type=float)
    arg_parser.add_argument( "gpx_file_folder", help="Path to folder with all GPX files for these pictures")
    arg_parser.add_argument( "destination_root", help="Root of destination directory (e.g., \"Q:\Lightroom\Images\")")
    return arg_parser.parse_args()


def _add_perf_timing(perf_timings, label, value):
    perf_timings['entries'].append(
        {
            'label' : label,
            'value' : value,
        }
    )
    perf_timings['total'] += value


def _enumerate_source_images(program_options):
    start_time = time.perf_counter()

    files_by_source_dir = {}

    print( "\nEnumerating source images")

    for curr_source_dir in program_options['source_dirs']:
        print( f"\tScanning \"{curr_source_dir}\" for RAW image files with extension " +
            f"\"{program_options['file_extension']}\"")

        image_files = []
        cumulative_bytes = 0
        for subdir, dirs, files in os.walk(curr_source_dir):
            #logging.debug(f"Found subdir {subdir}")
            for filename in files:
                if filename.lower().endswith( program_options['file_extension'] ):
                    file_absolute_path = os.path.join( curr_source_dir, subdir, filename)
                    #logging.debug( "Found image with full path: \"{0}\"".format(file_absolute_path))
                    file_size_bytes = os.path.getsize(file_absolute_path)
                    #logging.debug(f"File size of {file_absolute_path}: {file_size_bytes}")
                    cumulative_bytes += file_size_bytes
                    image_files.append(
                        {
                            'file_path'         : {
                                'absolute'          : file_absolute_path,
                                'relative'          : os.path.relpath( file_absolute_path, curr_source_dir )
                            },
                            'filesize_bytes'    : file_size_bytes,
                        }
                    )
                    #break
                else:
                    #logging.debug( "Skipping non-image file {0}".format(filename))
                    pass
        files_by_source_dir[ curr_source_dir ] = {
            'files'             : image_files,
            'cumulative_size'   : cumulative_bytes,
        }

    end_time = time.perf_counter()

    operation_time_seconds = end_time - start_time
    #logging.debug( f"Enumerate time: {(operation_time_seconds):.03f} seconds" )

    return {
        'source_dirs'               : files_by_source_dir,
        'operation_time_seconds'    : operation_time_seconds,
    }


def _display_perf_timings( perf_timings ):
    # Find longest label
    longest_label_len = len('Total')
    for entry in perf_timings['entries']:
        if len(entry['label']) > longest_label_len:
            longest_label_len = len(entry['label'])

    print( "\nPerformance data:\n" )

    for curr_entry in perf_timings['entries']:
        percentage_time = (curr_entry['value'] / perf_timings['total']) * 100.0
        print( f"\t{curr_entry['label']:>{longest_label_len}s} : {curr_entry['value']:>7.03f} seconds " +
               f"({percentage_time:5.01f}%)")

    total_label = "Total"
    print (f"\n\t{total_label:>{longest_label_len}s} : {perf_timings['total']:>7.03f} seconds" )


def _generate_source_manifest( source_dirs_info ):
    start_time = time.perf_counter()

    source_manifests = {}
    print( "\nGenerating source manifest")

    process_handles = []

    #  Queue for sending files needing timestamps to children
    files_to_process_queue = multiprocessing.Queue()

    # Queue that children use to write hash data information data back to parent
    file_hashes_queue = multiprocessing.Queue()

    # Event object that parent uses so children know when to exit
    parent_done_writing = multiprocessing.Event()

    for i in range(multiprocessing.cpu_count()):
        process_handle = multiprocessing.Process( target=_source_file_hash_worker,
                                                  args=(i+1, files_to_process_queue,
                                                        file_hashes_queue,
                                                        parent_done_writing) )

        process_handle.start()
        #logging.debug(f"Parent back from start on child process {i+1}")
        process_handles.append( process_handle )

    # Load up the queue with all the files we want hashes of
    file_count = 0
    for curr_source_dir in source_dirs_info:
        print( f"\tSource directory: {curr_source_dir}")
        source_manifests[ curr_source_dir ] = {}

        for curr_file in source_dirs_info[curr_source_dir]['files']:
            #print( f"\t\tFile: {curr_file}")

            files_to_process_queue.put(
                {
                    'source_dir'    : curr_source_dir,
                    'file_to_hash'  : curr_file,
                }
            )
            file_count += 1

    # Let the children know to not wait if queue is empty
    parent_done_writing.set()

    # We know how many files we wrote into the queue that children read out of. Now read
    #   same number of entries out of the processed data queue
    for i in range( file_count ):
        file_hash_data = file_hashes_queue.get()
        source_manifests[ file_hash_data['source_dir']][ file_hash_data['file_info']['file_path'] ['relative'] ] = {
            'filesize_bytes'    : file_hash_data['file_info']['filesize_bytes'],
            'hashes'          : file_hash_data['file_info']['hashes'],
        }

    # If there are two source directories provided, their two manifests best be identical
    if len( source_manifests.keys() ) == 2:
        source_dirs = sorted( source_manifests.keys() )
        if source_manifests[ source_dirs[0] ] != source_manifests[ source_dirs[1] ]:
            print( f"\tFATAL: manifests for the two source dirs {source_dirs} did not come out identical, issues")
        else:
            print( "\tThe two source directories have byte-identical contents -- as expected!")

    #print( json.dumps(source_manifests, indent=4, sort_keys=True))

    end_time = time.perf_counter()
    operation_time_seconds = end_time - start_time

    return {
        "operation_time_seconds"    : operation_time_seconds,
        "source_manifest"           : source_manifests[list(source_dirs_info.keys())[0]],
    }


def _source_file_hash_worker( worker_index, source_file_queue, hash_queue, parent_done_writing ):
    while True:
        try:
            # No need to wait, the queue was pre-loaded by the parent
            curr_file_entry = source_file_queue.get(timeout=0.1)
        except queue.Empty:
            if parent_done_writing.is_set():
                break
        #print( f"Child got file {json.dumps(curr_file_entry, indent=4, sort_keys=True)}" )
        with open( curr_file_entry['file_to_hash']['file_path']['absolute'], "rb") as file_to_hash_handle:
            file_bytes = file_to_hash_handle.read()
            curr_file_entry['file_to_hash']['hashes'] = {
                #"sha3_384"  :   hashlib.sha3_384(file_bytes).hexdigest(),
                "sha3_512"  :   hashlib.sha3_512(file_bytes).hexdigest(),
            }

            hash_queue.put(
                {
                    "source_dir"    : curr_file_entry['source_dir'],
                    "file_info"     : curr_file_entry['file_to_hash'],
                }
            )


def _extract_image_timestamps( program_options, source_image_manifest ):

    time_start = time.perf_counter()

    file_data = {}
    source_image_files = sorted(source_image_manifest.keys())
    file_count = len(source_image_files)

    print( f"\nStarting EXIF timestamp extraction for {file_count} files")

    start_time = time.perf_counter()

    process_handles = []

    #  Queue for sending files needing timestamps to children
    files_to_process_queue = multiprocessing.Queue(maxsize=file_count)

    # Queue that children use to write EXIF timestamp information data back to parent
    processed_file_queue = multiprocessing.Queue(maxsize=file_count)

    for i in range(multiprocessing.cpu_count()):
        process_handle = multiprocessing.Process( target=_exif_timestamp_worker,
                                                  args=(i+1, files_to_process_queue,
                                                        processed_file_queue,
                                                        program_options) )

        process_handle.start()
        #logging.debug(f"Parent back from start on child process {i+1}")
        process_handles.append( process_handle )

    # Load up the queue with all the files to process
    for curr_file in source_image_files:
        exif_worker_data = {
            'paths'             : {
                'absolute'          : os.path.join(program_options['source_dirs'][0], curr_file ),
                'relative'          : curr_file,
            },
            'manifest_data'     : source_image_manifest[curr_file],
        }
        logging.debug(f"About to write {json.dumps(exif_worker_data)} to the child queue")
        files_to_process_queue.put(exif_worker_data)

    # We know how many files we wrote into the queue that children read out of. Now read
    #   same number of entries out of the processed data queue
    for i in range( file_count ):
        exif_timestamp_data = processed_file_queue.get()
        source_image_manifest[ exif_timestamp_data['paths']['relative']]['timestamp'] = \
                exif_timestamp_data['timestamp']

    #logging.debug( f"Parent process has read out all {file_count} entries from results queue" )

    # Rejoin child threads
    while process_handles:
        curr_handle = process_handles.pop()
        #logging.debug("parent process waiting for child worker to rejoin")
        curr_handle.join()
        #logging.debug("child process has rejoined cleanly")

    #logging.debug("Parent process exiting, all EXIF timestamp work done")

    time_end = time.perf_counter()

    operation_time_seconds = time_end - time_start

    return {
        'operation_time_seconds'    : operation_time_seconds,
    }


def _exif_timestamp_worker( child_process_index, files_to_process_queue, processed_file_queue, program_options ):
    #print( f"Child {child_process_index} started")

    exiftool_tag_name = "EXIF:DateTimeOriginal"

    with exiftool.ExifTool(program_options['exiftool_path']) as exiftool_handle:

        while True:
            try:
                # No need to wait, the queue was pre-loaded by the parent
                curr_file_entry = files_to_process_queue.get( timeout=0.1 )
            except queue.Empty:
                # no more work to be done
                #print( f"Child {child_process_index} found queue empty on get, bailing from processing loop")
                break

            #print( f"Child {child_process_index} read processing entry from queue: " +
            #    json.dumps(curr_file_entry, indent=4, sort_keys=True) )

            absolute_path = curr_file_entry['paths']['absolute']

            exif_datetime = exiftool_handle.get_tag(exiftool_tag_name, absolute_path)

            # Create legit datetime object from string, note: not tz aware (yet)
            file_datetime_no_tz = datetime.datetime.strptime(exif_datetime, "%Y:%m:%d %H:%M:%S")

            # Do hour shift from timezone-unaware EXIF datetime to UTC
            shifted_datetime_no_tz = file_datetime_no_tz + datetime.timedelta(
                hours=-(program_options['file_timestamp_utc_offset_hours']))

            # Create TZ-aware datetime, as one should basically always strive to use
            file_datetime_utc = shifted_datetime_no_tz.replace(tzinfo=datetime.timezone.utc)

            # Create an

            processed_file_queue.put(
                {
                    'paths': {
                        'relative': curr_file_entry['paths']['relative'],
                    },
                    'timestamp': file_datetime_utc,
                }
            )


    #print( f"Child {child_process_index} exiting cleanly")


def _set_unique_destination_filename( source_file, file_data, program_options, filename_conflict_dict,
                                      destination_dirs_scanned ):
    #logging.debug( f"Setting destination filename for {source_file} under {args.destination_root}")

    # Folder structure is YYYY\YY-MM-DD\[unique filename]

    date_components = {
        'year'          : file_data['timestamp'].year,
        'date_iso8601'  : \
            f"{file_data['timestamp'].year:4d}-{file_data['timestamp'].month:02d}-{file_data['timestamp'].day:02d}",
    }

    # merge the date_component data into the file_data
    file_data.update( date_components )

    year_subfolder = os.path.join( program_options['destination_root'], str(date_components['year']))
    year_date_subfolder = os.path.join( year_subfolder, date_components['date_iso8601'] )

    file_data['destination_subfolders'] = {
        'year': year_subfolder,
        'date': year_date_subfolder,
    }

    # Have we added existing files in this subfolder to the conflict_dict already?
    if not year_date_subfolder in destination_dirs_scanned:
        if os.path.isdir( year_subfolder) is True and os.path.isdir( year_date_subfolder ) is True:
            # TODO: Enumerate all matching files in the directory
            glob_match_str = os.path.join( year_date_subfolder, f"*{matching_file_extension}" )
            #logging.debug( f"Glob match string: {glob_match_str}")

            files_matching_glob = glob.glob( glob_match_str )

            # add any files in this dir
            for curr_match in files_matching_glob:
                filename_conflict_dict[curr_match] = None

            #logging.debug( f"Added {len(files_matching_glob)} files from \"{year_date_subfolder}\" to conflict list")
        else:
            #logging.debug( f"Subfolder \"{year_date_subfolder}\" does not exist, added to list of dirs we have scanned" )
            pass

        # Regardless of which logic path we took, we can now say we've scanned that directory
        destination_dirs_scanned[ year_date_subfolder ] = None
    else:
        #logging.debug( f"Already scanned directory \"{year_date_subfolder}\", skipping")
        pass

    # Find first filename that doesn't exist in conflict list
    basename = os.path.basename( source_file )
    (basename_minus_ext, file_extension) = os.path.splitext(basename)

    test_file_path = os.path.join( year_date_subfolder, basename )
    index_append = None

    while test_file_path in filename_conflict_dict:
        # Need to come up with a non-conflicting name
        if index_append is None:
            index_append = 1
        else:
            index_append += 1

        next_attempt_name = os.path.join( year_date_subfolder,
                                          basename_minus_ext + f"-{index_append:04d}" + file_extension )

        logging.info( f"Found destination filename conflict with \"{test_file_path}\", trying \"{next_attempt_name}\"" )

        test_file_path = next_attempt_name

    logging.debug( f"Found unique destination filename: {test_file_path}")
    # Mark the final location for this file
    file_data[ 'unique_destination_file_path' ] = test_file_path

    # Add final location to our conflict list
    filename_conflict_dict[ test_file_path ] = None

    logging.debug( f"Updated file info:\n{json.dumps(file_data, indent=4, sort_keys=True, default=str)}")


def _set_destination_filenames( program_options, source_file_manifest ):

    start_time = time.perf_counter()

    sorted_files = sorted(source_file_manifest.keys())

    destination_filename_conflict_dict = {}
    destination_dirs_scanned = {}

    for curr_file in sorted_files:
        logging.debug(f"Getting size and unique destination filename for {curr_file}")
        #logging.debug(f"\tSize: {file_size} bytes")

        _set_unique_destination_filename( curr_file, source_file_manifest[curr_file],
            program_options, destination_filename_conflict_dict,
            destination_dirs_scanned )

    end_time = time.perf_counter()
    operation_time_seconds = end_time - start_time

    return {
        "operation_time_seconds"    : operation_time_seconds,
    }


def _main():
    args = _parse_args()
    program_options = {}

    if args.debug is False:
        log_level = logging.INFO
    else:
        log_level = logging.DEBUG
    logging.basicConfig( filename=args.logfile, level=log_level )
    #logging.basicConfig(level=log_level)

    program_options['source_dirs'] = args.sourcedir
    program_options['file_extension'] = args.fileext.lower()
    program_options['exiftool_path'] = args.exiftool
    program_options['file_timestamp_utc_offset_hours'] = args.file_timestamp_utc_offset_hours
    program_options['gpx_file_folder'] = args.gpx_file_folder
    program_options['destination_root'] = args.destination_root

    logging.debug( f"Program options: {json.dumps(program_options, indent=4, sort_keys=True)}" )

    perf_timings = {
        'total' : 0.0,
        'entries': [],
    }

    source_image_info = _enumerate_source_images(program_options)
    _add_perf_timing(perf_timings, 'Enumerating source images', source_image_info['operation_time_seconds'])

    manifest_info = _generate_source_manifest(source_image_info['source_dirs'])
    _add_perf_timing(perf_timings, 'Creating source image manifest', manifest_info['operation_time_seconds'])
    source_file_manifest = manifest_info['source_manifest']

    # Get timestamp for all image files
    timestamp_output = _extract_image_timestamps( program_options, source_file_manifest )
    _add_perf_timing( perf_timings, 'Extracting EXIF timestamps', timestamp_output['operation_time_seconds'])

    # Determine unique filenames
    print( "\nDetermining unique filenames in destination folder")
    set_destination_filenames_results = _set_destination_filenames( program_options, source_file_manifest )
    _add_perf_timing( perf_timings, 'Generating Unique Destination Filenames',
                      set_destination_filenames_results['operation_time_seconds'] )



    
    print( json.dumps( source_file_manifest, indent=4, sort_keys=True, default=str))

    # Do file copies

    # Do readback validation to make sure NAS writes worked

    # Write manifest file to each output directory with the info for the files in that directory

    # Geotag images into XMP

    # Pull geotags out of XMP and put into manifest files? Maybe?


    # Final perf print
    _display_perf_timings( perf_timings )


if __name__ == "__main__":
    _main()
