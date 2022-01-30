# import pprint
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
import shutil
import pathlib
import glob
import performance_timer
import random
import shutil
import xml.etree.ElementTree
import copy


def _parse_args():
    arg_parser = argparse.ArgumentParser(description="End of day script to validate data, geotag, copy to SSD")
    arg_parser.add_argument("--debug", help="Lower logging level from INFO to DEBUG", action="store_true" )
    arg_parser.add_argument("--logfile", help="Path to file where the app log will be created",
                            default="photo_end_of_day.log" )
    arg_parser.add_argument( "--singlethreaded", help="Disable multi-threading, do everything single threaded",
                             action="store_true" )
    arg_parser.add_argument( "--sourcedir_full",
                             help="Path to a source directory with the *full* set of contents",
                             required=True )

    # The "Action append" lets the parameter be specified multiple times, but it's still optional so may exist no times
    arg_parser.add_argument( "--sourcedir_partial",
                             help="Path to a source directory with a *partial* set of the contents",
                             action="append")
    arg_parser.add_argument( "fileext", help="File extension, e.g. \"NEF\", \"CR3\""  )
    arg_parser.add_argument("file_timestamp_utc_offset_hours",
                            help="Hours offset from UTC, e.g. EDT is -4, Afghanistan is +4.5",
                            type=float)
    arg_parser.add_argument( "gpx_file_folder", help="Path to folder with all GPX files for these pictures")
    arg_parser.add_argument( "laptop_destination_folder", help="Path to storage folder on laptop NVMe" )
    arg_parser.add_argument( "travel_storage_media_folder", nargs="+",
                             help="External travel storage media folder " + \
                                  "(e.g., SanDisk Extreme Pro 4TB, WD My Passport 4TB)")

    return arg_parser.parse_args()


def _scan_source_dir_for_images( curr_source_dir, program_options, source_list_type, directory_scan_results_queue ):
    image_files = []
    cumulative_bytes = 0
    #print(f"\tChild process starting walk of {curr_source_dir}")

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
    results_of_walk = {
        'source_dir'        : curr_source_dir,
        'source_list_type'  : source_list_type,
        'image_files_found' : image_files,
    }

    directory_scan_results_queue.put( results_of_walk )

    # Now can terminate child worker process cleanly, ready to rejoin with parent
    #print( f"\tChild process walking {curr_source_dir} terminated cleanly")


def _enumerate_source_images(program_options):
    start_time = time.perf_counter()

    print( "\nEnumerating source images")

    source_file_lists = {
        'full': {},
        'partial': None,
    }

    process_handles = []

    # Queue that children use to write results of directory scan back to parent
    directory_scan_results_queue = multiprocessing.Queue()

    # Fire off scanner for the directory with ALL the shots (e.g., 512 GB CFexpress Type B card with *all* the shots for the day)
    which_source_list = 'full'
    curr_handle = multiprocessing.Process(target=_scan_source_dir_for_images,
                                             args=(program_options['sourcedirs']['full'],
                                                   program_options,
                                                   which_source_list,
                                                   directory_scan_results_queue))

    curr_handle.start()
    print(f"\tScanning \"{program_options['sourcedirs']['full']}\" for RAW image files with extension " +
          f"\".{program_options['file_extension']}\" (full contents)")

    process_handles.append(curr_handle)

    directories_scanned = 1

    #Now fire off scanners for directories with partial contents (e.g., 256 GB SD card with half the day's shots) -- if any
    if program_options['sourcedirs']['partial']:
        which_source_list = 'partial'
        for curr_partial_dir in sorted(program_options['sourcedirs']['partial']):
            curr_handle = multiprocessing.Process(target=_scan_source_dir_for_images,
                                                     args=(curr_partial_dir,
                                                           program_options,
                                                           which_source_list,
                                                           directory_scan_results_queue))
            curr_handle.start()
            print(f"\tScanning \"{curr_partial_dir}\" for RAW image files with extension " +
                  f".\"{program_options['file_extension']}\" (partial contents)")

            process_handles.append(curr_handle)
            directories_scanned += 1

    # Children cannot terminate with a queue that's full, so drain the results queue
    for curr_index in range( directories_scanned ):
        scan_results = directory_scan_results_queue.get()
        #print( f"\tGot scan results for \"{scan_results['source_dir']}\" back")

        if scan_results['source_list_type'] == 'full':
            source_file_list_to_populate = source_file_lists['full']
        elif scan_results['source_list_type'] == 'partial':
            if source_file_lists['partial'] is None:
                source_file_lists['partial'] = {}
            source_file_list_to_populate = source_file_lists['partial']

        for curr_dir_scan_result in scan_results['image_files_found']:
            curr_relative_path_file_name = curr_dir_scan_result['file_path']['relative']

            if curr_relative_path_file_name in source_file_list_to_populate:
                raise ValueError(f"Found duplicate entry {curr_relative_path_file_name} in a file listing -- WTF!?!?!")

            # Put the confirmed-unique entry into the file list
            source_file_list_to_populate[curr_relative_path_file_name] = curr_dir_scan_result

    # Wait for all the children to rejoin cleanly
    while process_handles:
        rejoin_handle = process_handles.pop()
        #print( f"Waiting for {pprint.pformat(rejoin_handle)} to rejoin")
        rejoin_handle.join()
        #print( "Child process rejoined" )

    #print( "\tAll workers have rejoined cleanly" )


    # We've populated the full list, and the partial list (if we got partial dirs). Need to make sure
    #   the partial list matches the full list (only worry about relative filenames -- we'll check contents later when we're
    #   doing the manifests and are hashing the shit out of everything
    if source_file_lists['partial'] is not None:
        if sorted(source_file_lists['full'].keys()) != sorted( source_file_lists['partial'].keys() ):
            raise ValueError("List of relative files in full and partial lists did not match")
        print( "\tCool, the full file list and the combined results of all the partials give us same file list!")

    total_file_count = len( source_file_lists['full'].keys() )

    # Create a directory that maps all absolute file paths to the relative one they have in common
    reverse_file_list = {}
    for curr_relative_path in sorted( source_file_lists['full'] ):
        reverse_file_list[ source_file_lists['full'][curr_relative_path][ 'file_path'][ 'absolute' ] ] = curr_relative_path

    if source_file_lists['partial'] is not None:
        for curr_relative_path in sorted( source_file_lists['partial'] ):
            reverse_file_list[ source_file_lists['partial'][curr_relative_path]['file_path']['absolute'] ] = curr_relative_path

    end_time = time.perf_counter()

    operation_time_seconds = end_time - start_time
    #logging.debug( f"Enumerate time: {(operation_time_seconds):.03f} seconds" )

    print ( f"\tFound {total_file_count} .{program_options['file_extension']} files")

    return {
        'source_file_list'          : source_file_lists['full'],
        'reverse_map'               : reverse_file_list,
        'operation_time_seconds'    : operation_time_seconds,
    }


def _generate_source_manifest( reverse_file_map, source_file_list ):
    start_time = time.perf_counter()

    print( "\nGenerating source manifest")

    # Queue with filenames that hash workers will read and hash
    files_to_hash_queue = multiprocessing.Queue()

    # Queue that hash workers use to pass hash data back to parent
    completed_files_queue = multiprocessing.Queue()

    # Event to tell everyone when all the work is done
    all_hashes_read = multiprocessing.Event()

    # Launch hash workers
    hash_worker_handles = []
    for i in range( multiprocessing.cpu_count() ):
        process_handle = multiprocessing.Process( target=_hash_worker,
                                                  args=(i+1, files_to_hash_queue,
                                                        completed_files_queue,
                                                        all_hashes_read) )

        process_handle.start()
        #print(f"Parent back from start on hash worker {i+1}")
        hash_worker_handles.append( process_handle )
    #print( "All hash workers started" )

    source_manifest = {}

    # # Ahhh think I know. We set total hash count expected incorrectly.  need to do that based on reverse map
    # print( f"\tReverse map (all copies of all files) has {len(reverse_file_map.keys())} entries")
    # return

    # We know how many files we wrote into the queue that children read out of. Now read
    #   same number of entries out of the processed data queue
    hashes_received = 0
    #pprint.pprint( source_file_list)
    #return
    total_hash_count = len(reverse_file_map.keys())
    filenames_left_to_send = total_hash_count

    # We shuffle this list to ensure that we pull files from all source drives equally without needing to
    #       create lists of which files are on which drives. Easier and same end effect
    shuffled_absolute_path_list = list( reverse_file_map.keys() )
    random.shuffle( shuffled_absolute_path_list )

    total_file_count = len( source_file_list.keys() )
    #print( f"\tHashing {total_hash_count} files in order to confirm all copies of {total_file_count} manifest entries match")
    while hashes_received < total_hash_count:
        # Populate the filename hash list
        filenames_to_send = min( filenames_left_to_send, multiprocessing.cpu_count() )
        for i in range( filenames_to_send ):
            curr_absolute_path = shuffled_absolute_path_list.pop()
            files_to_hash_queue.put(
                {
                    'paths': {
                        'absolute': curr_absolute_path,
                        'relative': reverse_file_map[ curr_absolute_path ]
                    }
                }
            )
            filenames_left_to_send -= 1
            #print( "Sent filename")

        # Read from completed queue until empty, that'll keep workers busy
        if hashes_received < total_hash_count:
            while True:
                # Read the completed queue
                try:
                    file_hash_data = completed_files_queue.get_nowait()
                except queue.Empty:
                    break

                # If the relative path exists in the source manifest, make sure the hashes line up!
                relative_path = file_hash_data['paths']['relative']
                if relative_path in source_manifest:
                    if file_hash_data['hashes'] != source_manifest[relative_path]['hashes']:
                        raise ValueError( f"Got hash file mismatch on {relative_path}")
                else:
                    source_manifest[ relative_path ] = {
                        'hashes'            : file_hash_data['hashes'],
                        'filesize_bytes'    : source_file_list[ relative_path ]['filesize_bytes']
                    }

                hashes_received += 1
                #print( f"\tParent got hash {hashes_received}" )

                # Can now delete from reverse map, no longer needed
                del reverse_file_map[ file_hash_data['paths']['absolute'] ]
                file_hash_data = None
        #print("Parent done with iteration of reading hash results, going to try sending more data")

    #print( "Parent done reading and writing from queues")
    #print( f"\tGot {len(source_manifest.keys())} unique relative file paths out of total of {total_hash_count} source files")

    # Signal that the hash workers can now terminate cleanly
    #print( "\tAll hashes read by parent, signaling hash workers to come home")
    all_hashes_read.set()

    # Rejoin all processes we spawned

    while hash_worker_handles:
        curr_handle = hash_worker_handles.pop()
        curr_handle.join()
    #print("All hash workers rejoined")

    end_time = time.perf_counter()
    operation_time_seconds = end_time - start_time

    print (f"\tSource manifest created for all {total_file_count} unique image files, computed from {total_hash_count} separate files to make sure all copies match")

    return {
        "operation_time_seconds"    : operation_time_seconds,
        "source_manifest"           : source_manifest,
    }



def _hash_worker( hash_worker_index, source_filecontents_queue, contents_hash_queue, all_hashes_read ):
    while True:
        try:
            curr_file_entry = source_filecontents_queue.get(timeout=0.1)
        except queue.Empty:
            # Test to see if the parent has read all the data out, otherwise we're not done
            if all_hashes_read.is_set() is False:
                continue
            else:
                break

        with open( curr_file_entry['paths']['absolute'], "rb") as file_handle:
            file_bytes = file_handle.read()

        #print( f"Child {worker_index} got file info:\n{json.dumps(curr_file_entry, indent=4, sort_keys=True)}")

        computed_hashes = {
            #"sha3_384"  :   hashlib.sha3_384(file_bytes).hexdigest(),
            "sha3_512"  :   hashlib.sha3_512(file_bytes).hexdigest(),
        }

        #print( f"{curr_file_entry['absolute_path']}: {computed_hashes['sha3_512']}")

        contents_hash_queue.put(
            {
                "paths":  curr_file_entry['paths'],
                "hashes": computed_hashes,
            }
        )

        # Release our reference to the file entry
        curr_file_entry = None

    #print( f"Hash worker {hash_worker_index} terminating cleanly" )


def _extract_image_timestamps( program_options, source_image_manifest ):

    time_start = time.perf_counter()

    file_data = {}
    source_image_files = sorted(source_image_manifest.keys())
    file_count = len(source_image_files)

    print( f"\nExtracting EXIF timestamps from source images")

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
        logging.debug(f"Parent back from start on timestamp child process {i+1}")
        process_handles.append( process_handle )

    # Load up the queue with all the files to process
    for curr_file in source_image_files:
        exif_worker_data = {
            'paths'             : {
                'absolute'          : os.path.join(program_options['sourcedirs']['full'], curr_file ),
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

    logging.debug( f"Parent process has read out all {file_count} entries from results queue" )

    # Rejoin child threads
    while process_handles:
        curr_handle = process_handles.pop()
        #logging.debug("parent process waiting for child worker to rejoin")
        curr_handle.join()
        #logging.debug("child process has rejoined cleanly")

    #logging.debug("Parent process exiting, all EXIF timestamp work done")

    time_end = time.perf_counter()

    operation_time_seconds = time_end - time_start

    print( f"\tCompleted for {file_count} source image files")

    return {
        'operation_time_seconds'    : operation_time_seconds,
    }


def _exif_timestamp_worker( child_process_index, files_to_process_queue, processed_file_queue, program_options ):
    #print( f"Child {child_process_index} started")

    exiftool_tag_name = "EXIF:DateTimeOriginal"

    with exiftool.ExifTool() as exiftool_handle:

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


def _get_existing_files_in_destination( source_file_manifest, program_options ):

    time_start = time.perf_counter()

    print( f"\nStarting search for existing .{program_options['file_extension']} files under " +
           f"{program_options['laptop_destination_folder']}")

    existing_files = {}

    total_existing_file_count = 0

    # Get all the year/date combos in the source_file_manifest
    for curr_source_file_manifest_file in source_file_manifest:
        curr_source_file_manifest_entry = source_file_manifest[curr_source_file_manifest_file]

        #print( f"Source file entry for {curr_source_file_manifest_file}:\n" + json.dumps(curr_source_file_manifest_entry, indent=4, sort_keys=True, default=str))

        curr_year = curr_source_file_manifest_entry['timestamp'].year
        year_str = str(curr_year)
        curr_month = curr_source_file_manifest_entry['timestamp'].month
        curr_day = curr_source_file_manifest_entry['timestamp'].day

        date_string = f"{curr_year:04d}-{curr_month:02d}-{curr_day:02d}"

        #print( f"File: {curr_source_file_manifest_file}, date: \"{date_string}\"")

        if year_str not in existing_files:
            existing_files[year_str] = {}

        if date_string not in existing_files[year_str]:
            existing_files[year_str][date_string] = {}
            #print( f"\tCreated existing files entry for \"{year_str}\{date_string}\"" )

            #print( f"Folder to search: {folder_to_search}")
            folder_to_search = os.path.join(program_options['laptop_destination_folder'],
                                            year_str,
                                            date_string)

            if os.path.exists( folder_to_search ) and os.path.isdir( folder_to_search):

                matching_files = glob.glob( os.path.join( folder_to_search, f"*.{program_options['file_extension']}"))

                total_existing_file_count += len( matching_files )

                for curr_match in matching_files:
                    existing_files[year_str][date_string][curr_match] = None

                print( f"\tAdded {len(matching_files)} .{program_options['file_extension']} files from {folder_to_search}")

    time_end = time.perf_counter()
    operation_time_seconds = time_end - time_start

    print( f"\tFound total of {total_existing_file_count} pre-existing .{program_options['file_extension']} files")

    return {
        'operation_time_seconds'    : operation_time_seconds,
        'existing_files'            : existing_files,
    }



def _set_unique_destination_filename( source_file, file_data, program_options, existing_destination_files,
                                      destination_file_manifests ):
    # Folder structure is YYYY\YY-MM-DD\[unique filename]
    date_components = {
        'year'          : str( file_data['timestamp'].year ),

        # Can't we pull the date out of the timestmap and just ISO8601 this? I don't think we need to build our own
        'date_iso8601'  : \
            f"{file_data['timestamp'].year:4d}-{file_data['timestamp'].month:02d}-{file_data['timestamp'].day:02d}",
    }

    #print( f"\tTrying to find unique destination filename for {date_components['year']}\{date_components['date_iso8601']}\{source_file}")

    #print( "Existing destination files:\n" + json.dumps(existing_destination_files, indent=4, sort_keys=True))

    if date_components['year'] not in destination_file_manifests:
        destination_file_manifests[ date_components['year']] = {}

    if date_components['date_iso8601'] not in destination_file_manifests[ date_components['year'] ]:
        destination_file_manifests[ date_components['year'] ][ date_components['date_iso8601']] = {}
        #print( f"\t\tCreated destination file manifest entry for \"{date_components['year']}\{date_components['date_iso8601']}\"")

    file_data['destination_subfolder'] = os.path.join( date_components['year'], date_components['date_iso8601'] )

    manifest_for_this_file = destination_file_manifests[ date_components['year'] ][ date_components['date_iso8601']]
    existing_destination_files_in_dir = []
    if date_components['year'] in existing_destination_files and    \
            date_components['date_iso8601'] in existing_destination_files[date_components['year']]:
        for curr_existing_file in existing_destination_files[ date_components['year'] ][ date_components['date_iso8601']]:
            # Get just the filename out
            existing_destination_files_in_dir.append( os.path.basename( curr_existing_file) )
    else:
        print( "\tFound a file with a date not included in the existing destination files dictionary: " 
               f"\"{date_components['year']}\{date_components['date_iso8601']}\"" )

        print( "\tListing all entries in the existing destination files dictionary:")
        for curr_year in sorted(existing_destination_files):
            for curr_date in sorted(existing_destination_files[curr_year]):
                print( f"\t\tFound year/date combo {curr_year}\{curr_date}" )

        print("\tDone")

    # print( f"Existing files in {date_components['year']}\{date_components['date_iso8601']}:\n" +
    #     json.dumps( existing_destination_files_in_dir, indent=4, sort_keys=True) )

    # Find first filename that doesn't exist in the given destination manifest
    basename = os.path.basename( source_file )
    (basename_minus_ext, file_extension) = os.path.splitext(basename)

    test_filename = basename
    index_append = 0

    # If there is a pre-existing file with that name, or we're planning on creating a file with that name, find one that doesn't exist
    while test_filename in existing_destination_files_in_dir or test_filename in manifest_for_this_file:
        # Need to come up with a non-conflicting name
        index_append += 1

        next_attempt_name = f"{basename_minus_ext}_{index_append:04d}{file_extension}"

        logging.info( f"Found destination filename conflict with \"{test_filename}\", trying \"{next_attempt_name}\"" )

        test_filename = next_attempt_name

    #print( f"\t\tFound unique destination filename: {date_components['year']}\{date_components['date_iso8601']}\{test_filename}")
    # Mark the final location for this file
    file_data[ 'unique_destination_file_path' ] = os.path.join( date_components['year'],
                                                                date_components['date_iso8601'],
                                                                test_filename )

    # Record the XMP filename for this destination file
    (filename_no_extension, filename_extension) = os.path.splitext( test_filename )
    file_data[ 'xmp' ] = {
        'filename': os.path.join( date_components['year'],
                                  date_components['date_iso8601'],
                                  f"{filename_no_extension}.xmp" ),
    }

    # Update destination manifest
    manifest_for_this_file[ test_filename ] = file_data
    # Replace the timestamp object with a ISO format string
    #manifest_for_this_file[ test_filename ][ 'timestamp'] = \
        #manifest_for_this_file[ test_filename ][ 'timestamp'].isoformat()

    #logging.debug( f"Updated file info:\n{json.dumps(file_data, indent=4, sort_keys=True, default=str)}")


def _set_destination_filenames( program_options, source_file_manifest, existing_destination_files ):

    print( "\nDetermining unique filenames in destination folder")

    #raise ValueError("Not actually detecting duplicates in destination directory anymore, what's up?")

    start_time = time.perf_counter()

    sorted_files = sorted(source_file_manifest.keys())

    destination_file_manifests = {}

    for curr_file in sorted_files:
        logging.debug(f"Getting size and unique destination filename for {curr_file}")

        _set_unique_destination_filename( curr_file, source_file_manifest[curr_file],
            program_options, existing_destination_files, destination_file_manifests )

    end_time = time.perf_counter()
    operation_time_seconds = end_time - start_time

    print( f"\tUnique filenames set for all {len(sorted_files)} destination image files" )

    return {
        "operation_time_seconds"        : operation_time_seconds,
        'destination_file_manifests'    : destination_file_manifests,
    }


def _do_file_copies_to_laptop( program_options, source_file_manifest ):

    print( f"\nCopying files from source \"{program_options['sourcedirs']['full']}\" to destination \"{program_options['laptop_destination_folder']}\"")

    start_time = time.perf_counter()

    file_count = len( source_file_manifest.keys() )

    for curr_source_file in source_file_manifest:
        # print( f"Worker {worker_index} doing copy for {curr_source_file}" )
        curr_file_entry = source_file_manifest[curr_source_file]

        # Do we need to make either of the subfolders (YYYY/YYYY-MM-DD)?
        curr_folder = os.path.join( program_options['laptop_destination_folder'], curr_file_entry['destination_subfolder'] )
        try:
            pathlib.Path(curr_folder).mkdir(parents=True, exist_ok=True)
        except:
            print(f"Exception thrown in creating dirs for {curr_folder}")

        # Remove the destination subfolder section out of the manifest, it's no longer needed
        del curr_file_entry['destination_subfolder']

        # Attempt copy
        try:
            dest_path = os.path.join( program_options['laptop_destination_folder'], curr_file_entry['unique_destination_file_path'] )
            source_absolute_path = os.path.join( program_options['sourcedirs']['full'], curr_source_file)
            shutil.copyfile(source_absolute_path, dest_path)
            #print( f"\tCopied \"{source_absolute_path}\" -> \"{dest_path}\" successfully")

        except:
            print(f"Exception thrown when copying {curr_file_entry['file_path']}")

    end_time = time.perf_counter()
    operation_time_seconds = end_time - start_time

    print( "\tFile copies completed")

    return {
        "operation_time_seconds"        : operation_time_seconds,
    }


def _do_readback_validation( source_file_manifest, program_options ):
    start_time = time.perf_counter()

    process_handles = []

    #  Queue for sending files needing validation  to children
    files_to_verify_queue = multiprocessing.Queue()

    # Event object that parent uses so children know when to stop reading
    parent_done_writing = multiprocessing.Event()

    for i in range(multiprocessing.cpu_count()):
        process_handle = multiprocessing.Process( target=_validation_worker,
                                                  args=(i+1, files_to_verify_queue, parent_done_writing) )
        process_handle.start()
        #logging.debug(f"Parent back from start on child process {i+1}")
        process_handles.append( process_handle )

    # Load up the queue with all the files to process
    for curr_file in source_file_manifest:
        curr_file_info = source_file_manifest[curr_file]
        validation_worker_data = {
            'file_path'         : os.path.join( program_options['laptop_destination_folder'],
                                                curr_file_info['unique_destination_file_path'] ),
            'filesize_bytes'    : curr_file_info['filesize_bytes'],
            'hashes'            : curr_file_info['hashes'],
        }
        #print(f"About to write {json.dumps(validation_worker_data)} to the child queue")
        files_to_verify_queue.put(validation_worker_data)

    parent_done_writing.set()

    # Just wait until all child processes rejoin
    while process_handles:
        curr_process = process_handles.pop()
        curr_process.join()

    end_time = time.perf_counter()
    operation_time_seconds = end_time - start_time

    return {
        "operation_time_seconds"    : operation_time_seconds,
    }


def _validation_worker( worker_index, files_to_verify_queue, parent_done_writing ):
    while True:
        try:
            # No need to wait, the queue was pre-loaded by the parent
            curr_file_entry = files_to_verify_queue.get(timeout=0.1)
        except queue.Empty:
            # is it because the parent is done writing?
            if parent_done_writing.is_set():
                break

        #print( f"Child {worker_index} validating file {curr_file_entry['file_path']}")

        #print( f"Full deets:\n" + json.dumps(curr_file_entry, indent=4, sort_keys=True) )

        with open( curr_file_entry['file_path'], "rb" ) as verify_file_handle:
            file_bytes = verify_file_handle.read()
            computed_digest = hashlib.sha3_512(file_bytes).hexdigest()
            if computed_digest != curr_file_entry['hashes']['sha3_512']:
                print( f"FATAL: file {curr_file_entry['file_path']} does not have expected hash from manifest")
                #print( f"Before copy: {curr_file_entry['hashes']['sha3_512']}\n After copy: {computed_digest}")
            else:
                #print( f"{curr_file_entry['file_path']} passed its verify hash check ({curr_file_entry['hashes']['sha3_512']})")
                pass

    # Okay to just cleanly fall out and exit


def _create_xmp_files( destination_file_manifests, program_options ):
    start_time = time.perf_counter()

    #  Queue for sending files needing XMP files children
    xmp_file_queue = multiprocessing.Queue()

    parent_done_writing = multiprocessing.Event()

    #print( f"Destination manifests: {json.dumps(destination_file_manifests, indent=4, sort_keys=True, default=str)}" )

    images_written_to_queue = 0

    process_handles = []

    # Fire up the child processes
    for i in range(multiprocessing.cpu_count()):
    #for i in range(1):
        process_handle = multiprocessing.Process( target=_xmp_creation_worker,
                                                  args=(i+1, xmp_file_queue, parent_done_writing, program_options ) )
        process_handle.start()
        #logging.debug(f"Parent back from start on child process {i+1}")
        process_handles.append( process_handle )

    for curr_year_folder in sorted( destination_file_manifests ):
        for curr_date_folder in sorted( destination_file_manifests[ curr_year_folder ] ):
            curr_manifest = destination_file_manifests[ curr_year_folder ][ curr_date_folder ]
            #print( f"Processing manifest for {curr_date_folder}" )
            for curr_manifest_filename in sorted( curr_manifest ):
                xmp_file_queue.put(
                    {
                        'year'                  : curr_year_folder,
                        'date'                  : curr_date_folder,
                        'filename'              : curr_manifest_filename,
                        'xmp_generation_info'   : curr_manifest[curr_manifest_filename],
                    }
                )
                images_written_to_queue += 1

    parent_done_writing.set()


    # Wait for all child processes to re-join with parent
    while process_handles:
        curr_handle = process_handles.pop()
        curr_handle.join()

    end_time = time.perf_counter()
    operation_time_seconds = end_time - start_time

    return {
        "operation_time_seconds"    : operation_time_seconds,
    }


def _do_xmp_cleanup_canon_rf_24_105_lens_id( generated_xmp_file ):
    # ExifTool creates the following section in the XMP for a Canon RF 24-105 f4-7.1 IS STM lens:
    #
    #  <rdf:Description rdf:about=''
    #   xmlns:aux='http://ns.adobe.com/exif/1.0/aux/'>
    #   <aux:Lens>24.0 - 105.0 mm</aux:Lens>
    #  </rdf:Description>
    #
    # That is parsed very poorly by Lightroom, and it tags the Lens as "24," which doesn't look good in
    #   Lightroom and would look even worse in Flickr
    with open( generated_xmp_file, "r") as xmp_handle:
        xmp_contents = xmp_handle.read()

    # See if we have a Canon EOS R5 with that lens before making any changes
    identifying_aux_section_string = " <rdf:Description rdf:about=''\n" + \
        "  xmlns:aux='http://ns.adobe.com/exif/1.0/aux/'>\n" + \
        "  <aux:Lens>24</aux:Lens>\n" + \
        "  <aux:LensID>61182</aux:LensID>\n" + \
        " </rdf:Description>"
    identifying_strings = [
        "<tiff:Model>Canon EOS R5</tiff:Model>",
        identifying_aux_section_string,
    ]

    if all( curr_identifying_string in xmp_contents for curr_identifying_string in identifying_strings ):
        #print( f"File {generated_xmp_file} is from a Canon EOS R5 with RF 24-105mm f/4-7.1 IS STM lens")

        replaced_aux_section_string = """ <rdf:Description rdf:about=''
  xmlns:aux='http://ns.adobe.com/exif/1.0/aux/'>
  <aux:Lens>RF24-105mm F4-7.1 IS STM</aux:Lens>
  <aux:LensInfo>24/1 105/1 0/0 0/0</aux:LensInfo>
  <aux:LensID>61182</aux:LensID>
 </rdf:Description>"""

        # Open the XMP up and replace the aux section entirely
        with open( generated_xmp_file, "w") as fixed_handle:
            fixed_handle.write( xmp_contents.replace(identifying_aux_section_string, replaced_aux_section_string))

    else:
        #print(f"File {generated_xmp_file} is NOT from a Canon EOS R5 with RF 24-105mm f/4-7.1 IS STM lens")
        #print( "XMP Contents:\n" + xmp_contents)
        pass


def _cleanup_xmp( generated_xmp_file ):
    _do_xmp_cleanup_canon_rf_24_105_lens_id( generated_xmp_file )


def _xmp_creation_worker( worker_index, files_to_create_xmp_files_queue, parent_done_writing, program_options ):
    gpx_wildcard = os.path.join(program_options['gpx_file_folder'], "*.gpx")

    with exiftool.ExifTool() as exiftool_handle:
        while True:
            try:
                # No need to wait, the queue was pre-loaded by the parent
                curr_image_to_create_xmp_for = files_to_create_xmp_files_queue.get( timeout=0.1 )
            except queue.Empty:
                if parent_done_writing.is_set():
                    break

            #print( "Child asked to create XMP for:\n" + json.dumps( curr_image_to_create_xmp_for, indent=4, sort_keys=True,
            #                                                default=str))

            raw_file_absolute_path = os.path.join(program_options['laptop_destination_folder'],
                                                  curr_image_to_create_xmp_for['xmp_generation_info']['unique_destination_file_path'] )

            (filename_no_extension, filename_extension) = os.path.splitext( raw_file_absolute_path )

            expected_xmp_file = os.path.join( program_options['laptop_destination_folder'],
                                              curr_image_to_create_xmp_for['year'],
                                              curr_image_to_create_xmp_for['date'], f"{filename_no_extension}.xmp" )

            exiftool_parameters= (
                # Point it towards the RAW file we want an XMP file for
                exiftool.fsencode(raw_file_absolute_path),

                # Specify output to XMP. I'm not sure how it figured out it shoulduse XMP. Is -o automatically XMP?
                #   No, exiftool extracts type of output by extension given to the OUTFILE parameter
                "-o".encode(),
                "%d%f.xmp".encode(),
            )

            #print("Running EXIFTool XMP generation command")
            execute_output = exiftool_handle.execute( *exiftool_parameters )
            #print("Back from EXIFTool")

            # Do any necessary cleanup to the newly-generated XMP file to make for cleaner import
            #   into Lightroom (e.g., fixing a lens identifier for Canon RF 24-105mm f/4-7.1 IS STM)
            _cleanup_xmp( expected_xmp_file )

            # Now adding geotags to the XMP file -- note we're making the first-stage XMP our source,
            #   not the RAW image -- we don't want to write tags to the RAW file, just the XMP
            exiftool_params = (
                #"-v2".encode(),
                "-geotag".encode(),
                gpx_wildcard.encode(),
                "-geotime<${DateTimeOriginal}+00:00".encode(),
                exiftool.fsencode( expected_xmp_file ),
                "-o".encode(),
                "%d%f_geocode.xmp".encode(),
            )

            execute_output = exiftool_handle.execute(*exiftool_params)
            #print(f"\t\tGeotagged into {basename_minus_ext}_geocode.xmp\n")
            #print( f"Output:{execute_output.decode()}")

            # Now overwrite the first XMP file with the geotagged version
            geocoded_xmp_path = os.path.join( program_options['laptop_destination_folder'],
                                              curr_image_to_create_xmp_for['year'],
                                              curr_image_to_create_xmp_for['date'],
                                              f"{filename_no_extension}_geocode.xmp" )

            shutil.move( geocoded_xmp_path, expected_xmp_file )


    #print( "Child exiting cleanly")


def _copy_to_external_storage_worker( program_options, curr_travel_storage_media_folder, source_file_manifest):
    print( f"\tCreating travel media storage copy at \"{curr_travel_storage_media_folder}\"" )
    shutil.copytree( program_options['laptop_destination_folder'], curr_travel_storage_media_folder,
                     dirs_exist_ok=True)
    print( f"\tTravel media storage copy \"{curr_travel_storage_media_folder}\" created successfully")


def _copy_files_to_external_storage( program_options, source_file_manifest ):
    print( "\nCopying staged files from laptop NVMe to external storage travel media")

    start_time = time.perf_counter()
    process_handles = []
    #print( "Program options: " + json.dumps(program_options, indent=4, sort_keys=True) )
    for curr_travel_storage_media_folder in program_options['travel_storage_media_folders']:

        copy_process_handle = multiprocessing.Process(target=_copy_to_external_storage_worker,
                                             args=(program_options,
                                                   curr_travel_storage_media_folder,
                                                   source_file_manifest))
        copy_process_handle.start()
        #print(f"\tStarted copy of staged files to external travel storage media folder \"{curr_travel_storage_media_folder}\" ")

        process_handles.append(copy_process_handle)

    while process_handles:
        curr_join_handle = process_handles.pop()
        curr_join_handle.join()
    #print("\tAll copy processes have finished and rejoined with parent")

    print( "\tAll copies to travel media complete")

    end_time = time.perf_counter()
    operation_time_seconds = end_time - start_time

    return operation_time_seconds




def _update_manifest_with_geotags( program_options, destination_file_manifests ):
    start_time = time.perf_counter()

    for curr_year_folder in sorted( destination_file_manifests ):
        for curr_date_folder in sorted( destination_file_manifests[ curr_year_folder ] ):
            for curr_manifest_filename in sorted( destination_file_manifests[ curr_year_folder ][curr_date_folder] ):

                curr_manifest_entry = destination_file_manifests[curr_year_folder][curr_date_folder][curr_manifest_filename]
                xmp_path = os.path.join(program_options['laptop_destination_folder'],
                                        curr_manifest_entry['xmp']['filename'])

                # Pull the geotags out of the XMP file
                xmp_tree = xml.etree.ElementTree.parse(xmp_path)
                root = xmp_tree.getroot()

                xml_namespaces = {
                    'exif'  : 'http://ns.adobe.com/exif/1.0/',
                }
                gps_alt = root.find( ".//exif:GPSAltitude", xml_namespaces)
                gps_lat = root.find( ".//exif:GPSLatitude", xml_namespaces)
                gps_lon = root.find( ".//exif:GPSLongitude", xml_namespaces )

                # Make readable versions of the geotag data, as it's... not great
                (alt_numerator, alt_denominator) = gps_alt.text.split( "/" )
                alt_numerator = int( alt_numerator )
                alt_denominator = int( alt_denominator )

                latitude_hemisphere = gps_lat.text[-1]
                if latitude_hemisphere == "N":
                    latitude_multiplier = 1
                elif latitude_hemisphere == "S":
                    latitude_multiplier = -1
                else:
                    raise ValueError("Invalid latitude hemisphere, neither N nor S")

                (lat_degrees, lat_minutes) = gps_lat.text.split( ",")
                readable_lat = latitude_multiplier * (int(lat_degrees) + (float(lat_minutes[:-1]) / 60.0))

                longitude_hemisphere = gps_lon.text[-1]
                if longitude_hemisphere == "W":
                    longitude_multiplier = -1
                elif longitude_hemisphere == "E":
                    longitude_multiplier = 1
                else:
                    raise ValueError("Invalid longitude hemisphere, neither W nor E")
                (lon_degrees, lon_minutes) = gps_lon.text.split(",")
                readable_lon = longitude_multiplier * (int(lon_degrees) + (float(lon_minutes[:-1]) / 60.0))

                curr_manifest_entry[ 'geotag_info'] = {
                    'latitude'  : {
                        'value'     : str(round(readable_lat, 6)),
                        'unit'      : "WGS84 degrees",
                    },
                    'longitude' : {
                        'value'     : str(round(readable_lon, 6)),
                        'unit'      : "WGS84 degrees",
                    },
                    'altitude'  : {
                        'value'     : str(round(alt_numerator / alt_denominator, 2)),
                        'unit'      : 'meters above sea level',
                    }
                }

                # Create a hash of the XMP file and store it in the manifest

                with open(xmp_path, "rb") as xmp_file_handle:
                    file_bytes = xmp_file_handle.read()
                    computed_digest = hashlib.sha3_512(file_bytes).hexdigest()
                    curr_manifest_entry['xmp']['sha3_512'] = computed_digest

                #print( f"Updated manifest entry for {curr_year_folder}\{curr_date_folder}\{curr_manifest_filename}:")
                #print( json.dumps(curr_manifest_entry, indent=4, sort_keys=True, default=str) )

    end_time = time.perf_counter()
    operation_time_seconds = end_time - start_time

    return {
        'operation_time_seconds': operation_time_seconds,
    }

def _write_manifest_files( program_options, destination_file_manifests ):
    start_time = time.perf_counter()

    for curr_year_folder in sorted( destination_file_manifests ):
        for curr_date_folder in sorted( destination_file_manifests[ curr_year_folder ] ):
            curr_day_manifest = destination_file_manifests[curr_year_folder][curr_date_folder]

            # See if there's an existing manifest for that date, if so, read it in and merge it with our manifest
            target_manifest_path = os.path.join( program_options['laptop_destination_folder'],
                                             curr_year_folder,
                                             curr_date_folder,
                                             f"tdo_photo_manifest_{curr_date_folder}.json" )
            if os.path.isfile( target_manifest_path ):
                with open( target_manifest_path, "r") as existing_manifest_handle:
                    existing_manifest = json.load( existing_manifest_handle )
                write_manifest = copy.deepcopy( curr_day_manifest )
                write_manifest.update( existing_manifest )
            else:
                write_manifest = curr_day_manifest

            with open( target_manifest_path, "w" ) as new_or_updated_manifest_handle:
                json.dump( write_manifest, new_or_updated_manifest_handle, indent=4, sort_keys=True,
                           default=str)

    end_time = time.perf_counter()
    operation_time_seconds = end_time - start_time

    return {
        'operation_time_seconds': operation_time_seconds,
    }


def _verify_travel_media_file_worker( worker_num, program_options, hash_verification_queue,
                                      hash_checked_queue, parent_done_writing ):
    queue_timeout = 0.1

    while True:
        try:
            file_to_verify_entry = hash_verification_queue.get(timeout=queue_timeout)
        except queue.Empty:
            if parent_done_writing.is_set():
                break
            else:
                continue

        file_path = file_to_verify_entry['absolute_path']
        with open( file_path, "rb") as file_handle:
            file_bytes = file_handle.read()

        #print( f"Child {worker_index} got file info:\n{json.dumps(curr_file_entry, indent=4, sort_keys=True)}")

        computed_hashes = {
            #"sha3_384"  :   hashlib.sha3_384(file_bytes).hexdigest(),
            "sha3_512"  :   hashlib.sha3_512(file_bytes).hexdigest(),
        }

        #print( f"{curr_file_entry['absolute_path']}: {computed_hashes['sha3_512']}")

        hash_checked_queue.put(
            {
                "absolute_path"     : file_path,
                "hash_correct"      : computed_hashes == file_to_verify_entry['hashes'],
            }
        )

        #print( f"\tWorker sent results for {file_path} back to parent")

        # Release our reference to the file entry
        file_to_verify_entry = None
        file_bytes = None

    # If we break out of the loop, we're exiting normally


def _verify_travel_media_copies( program_options, destination_file_manifests ):
    start_time = time.perf_counter()

    # Queue that parent writes files needing validation to children
    hash_verification_queue = multiprocessing.Queue()

    # Queue that children use to write results of file verification scan back to parent
    hash_checked_queue = multiprocessing.Queue()

    parent_done_writing = multiprocessing.Event()

    process_handles = []
    #print( "Program options: " + json.dumps(program_options, indent=4, sort_keys=True) )
    for i in range(multiprocessing.cpu_count()):

        verify_process_handle = multiprocessing.Process(target=_verify_travel_media_file_worker,
                                             args=(i+1, program_options,
                                                   hash_verification_queue,
                                                   hash_checked_queue, parent_done_writing))
        verify_process_handle.start()

        process_handles.append(verify_process_handle)

    files_to_verify_list = []
    count_verify_checks_remaining = 0

    for curr_year_folder in sorted( destination_file_manifests ):
        for curr_date_folder in sorted( destination_file_manifests[ curr_year_folder ] ):
            for curr_manifest_filename in sorted( destination_file_manifests[ curr_year_folder ][curr_date_folder] ):

                curr_manifest_entry = destination_file_manifests[curr_year_folder][curr_date_folder][curr_manifest_filename]

                for curr_travel_media_folder in program_options['travel_storage_media_folders']:
                    files_to_verify_list.append(
                        {
                            'absolute_path'     : os.path.join( curr_travel_media_folder,
                                                           curr_year_folder,
                                                           curr_date_folder,
                                                           curr_manifest_filename ),

                            'hashes'            : curr_manifest_entry['hashes'],
                        }
                    )
                    count_verify_checks_remaining += 1

                    files_to_verify_list.append(
                        {
                            'absolute_path': os.path.join(curr_travel_media_folder,
                                                          curr_manifest_entry['xmp']['filename'] ),

                            'hashes': {
                                'sha3_512': curr_manifest_entry['xmp']['sha3_512']
                            },
                        }
                    )

                    count_verify_checks_remaining += 1

    # Loop through files to send out, alternating with reading all results out of queue
    while files_to_verify_list or count_verify_checks_remaining > 0:
        # Find out how many entries to write to children
        entries_to_send = min( len(files_to_verify_list), multiprocessing.cpu_count() )

        for i in range( entries_to_send ):
            curr_entry_to_send = files_to_verify_list.pop()
            hash_verification_queue.put( curr_entry_to_send )
            #print( f"Parent sent {curr_entry_to_send['absolute_path']} to verify queue")
            if len(files_to_verify_list) == 0:
                parent_done_writing.set()
                #print( "Parent marked done on sending data")

        while True:
            try:
                child_result = hash_checked_queue.get_nowait()
            except queue.Empty:
                break

            if child_result['hash_correct'] is False:
                print( f"ERROR: hash check failed for {child_result['absolute_path']}")
            count_verify_checks_remaining -= 1

    while process_handles:
        curr_rejoin_handle = process_handles.pop()
        curr_rejoin_handle.join()

    #print( "\tAll children have rejoined, checks complete")

    end_time = time.perf_counter()
    operation_time_seconds = end_time - start_time

    return operation_time_seconds


def _main():
    args = _parse_args()
    program_options = {}

    if args.debug is False:
        log_level = logging.INFO
    else:
        log_level = logging.DEBUG
    logging.basicConfig( filename=args.logfile, level=log_level )
    #logging.basicConfig(level=log_level)

    program_options['sourcedirs'] = {
        'full'      : args.sourcedir_full,
        'partial'   : args.sourcedir_partial,
    }

    program_options['file_extension'] = args.fileext.lower()
    program_options['file_timestamp_utc_offset_hours'] = args.file_timestamp_utc_offset_hours
    program_options['gpx_file_folder'] = args.gpx_file_folder
    program_options['laptop_destination_folder'] = args.laptop_destination_folder
    program_options['travel_storage_media_folders'] = args.travel_storage_media_folder

    logging.debug( f"Program options: {json.dumps(program_options, indent=4, sort_keys=True)}" )

    perf_timer = performance_timer.PerformanceTimer()

    source_image_info = _enumerate_source_images(program_options)
    perf_timer.add_perf_timing( 'Enumerating source images', source_image_info['operation_time_seconds'])

    #print( "File list to create source manifest:\n" + json.dumps(source_image_info['file_list'], indent=4, sort_keys=True) )
    #print( "Reverse map for hashing:\n" + json.dumps(source_image_info['reverse_map'], indent=4) )

    manifest_info = _generate_source_manifest( source_image_info['reverse_map'],
                                               source_image_info['source_file_list'] )
    # Delete the reverse map, don't need it anymore
    del source_image_info['reverse_map']
    perf_timer.add_perf_timing(  'Creating source image manifest', manifest_info['operation_time_seconds'])
    source_file_manifest = manifest_info['source_manifest']

    # Get timestamp for all image files
    timestamp_output = _extract_image_timestamps( program_options, source_file_manifest )
    perf_timer.add_perf_timing( 'Extracting EXIF timestamps', timestamp_output['operation_time_seconds'])

    # Enumerate files already in the destination directory
    destination_files_results = _get_existing_files_in_destination( source_file_manifest, program_options )
    perf_timer.add_perf_timing( "Listing existing files in destination folder",
                                destination_files_results['operation_time_seconds'] )
    existing_destination_files = destination_files_results['existing_files']

    # Determine unique filenames
    set_destination_filenames_results = _set_destination_filenames( program_options, source_file_manifest,
                                                                    existing_destination_files )
    perf_timer.add_perf_timing( 'Generating unique destination filenames',
                      set_destination_filenames_results['operation_time_seconds'] )
    destination_file_manifests = set_destination_filenames_results['destination_file_manifests']

    # Do file copies to laptop NVMe SSD
    copy_operation_results = _do_file_copies_to_laptop(program_options, source_file_manifest)
    perf_timer.add_perf_timing( 'Copying RAW files to laptop NVMe',
                     copy_operation_results['operation_time_seconds'])

    # Do readback validation to make sure all writes to laptop worked
    print("\nReading files back from laptop SSD to verify contents still match original hash")
    verify_operation_results = _do_readback_validation( source_file_manifest, program_options )
    print( "\tDone")
    perf_timer.add_perf_timing( 'Validating all writes to laptop NVMe SSD are still byte-identical to source',
        verify_operation_results['operation_time_seconds'])

    # Create XMP sidecar files
    print("\nCreating geotagged XMP sidecar files for all RAW images")
    xmp_generation_results = _create_xmp_files( destination_file_manifests, program_options )
    print( "\tDone")
    perf_timer.add_perf_timing(  'Generating geotagged XMP files', xmp_generation_results['operation_time_seconds'])

    # Pull geotags out of XMP and store in manifest
    print( "\nUpdating manifest with geotag and XMP hash data")
    geotag_and_timestamp_manifest_update_results = _update_manifest_with_geotags( program_options,
                                                                                  destination_file_manifests )
    print( "\tDone" )
    perf_timer.add_perf_timing( "Adding geotags and XMP file hashes to manifest", geotag_and_timestamp_manifest_update_results['operation_time_seconds'])

    # Create (or update) daily manifest files
    print( "\nWriting or updating per-day manifest files" )
    manifest_write_results = _write_manifest_files( program_options, destination_file_manifests )
    print( "\tDone")
    perf_timer.add_perf_timing( "Writing per-day manifest files to disk",
                                manifest_write_results['operation_time_seconds'] )

    # Copy from laptop to external storage
    external_copies_time_seconds = _copy_files_to_external_storage( program_options, source_file_manifest )
    perf_timer.add_perf_timing( 'Copying all files from laptop to all travel storage media devices',
                                 external_copies_time_seconds )

    # Validate external storage copies
    print( "\nVerifying all travel media copies")
    travel_media_verify_time_seconds = _verify_travel_media_copies( program_options, destination_file_manifests )
    print( "\tDone")
    perf_timer.add_perf_timing( "Verifying all travel media copies match original hashes",
                                travel_media_verify_time_seconds )

    # Final perf print
    print( "" )
    perf_timer.display_performance()


if __name__ == "__main__":
    _main()