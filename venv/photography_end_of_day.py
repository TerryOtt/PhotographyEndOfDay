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
import shutil
import pathlib
import glob
import tempfile
import zipfile
import performance_timer
import gpxpy
import xml.etree.ElementTree


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
    arg_parser.add_argument( "exiftool", help="Path to the exiftool utility" )
    arg_parser.add_argument("file_timestamp_utc_offset_hours",
                            help="Hours offset from UTC, e.g. EDT is -4, Afghanistan is +4.5",
                            type=float)
    arg_parser.add_argument( "gpx_file_folder", help="Path to folder with all GPX files for these pictures")
    arg_parser.add_argument( "laptop_destination_folder", help="Path to storage folder on laptop NVMe" )

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
          f".\"{program_options['file_extension']}\" (full contents)")

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
        print(f"Parent back from start on hash worker {i+1}")
        hash_worker_handles.append( process_handle )
    print( "All hash workers started" )

    source_manifest = {}

    # We know how many files we wrote into the queue that children read out of. Now read
    #   same number of entries out of the processed data queue
    hashes_received = 0
    #pprint.pprint( source_file_list)
    #return
    total_file_count = len(source_file_list.keys())
    filenames_left_to_send = total_file_count
    sorted_absolute_path_list = list( reverse_file_map.keys() )
    sorted_absolute_path_list.sort()
    while hashes_received < total_file_count:
        # Populate the filename hash list
        filenames_to_send = min( filenames_left_to_send, multiprocessing.cpu_count() )
        for i in range( filenames_to_send ):
            curr_absolute_path = sorted_absolute_path_list.pop()
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
        if hashes_received < total_file_count:
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
                        'hashes': file_hash_data['hashes'],
                        'filesize_bytes'    : source_file_list[ relative_path ]['filesize_bytes']
                    }

                hashes_received += 1
                #print( f"Parent got hash {hashes_received}" )

                # Can now delete from reverse map, no longer needed
                del reverse_file_map[ file_hash_data['paths']['absolute'] ]
                file_hash_data = None
        #print("Parent done with iteration of reading hash results, going to try sending more data")

    #print( "Parent done reading and writing from queues")

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

    print (f"\tSource manifest created for all {len(source_file_list.keys())} image files")

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


def _geocode_images( program_options, source_file_manifests ):
    print( "\nGeocoding images" )

    time_start = time.perf_counter()

    print( "\tReading GPX files" )

    gpx_file_wildcard = os.path.join( program_options['gpx_file_folder'], '*.gpx' )
    gpx_files = glob.glob( gpx_file_wildcard )

    gpx_file_data = []

    for curr_gpx_file in gpx_files:
        with open( curr_gpx_file, "r" ) as gpx_file_handle:
            gpx_data = gpxpy.parse( gpx_file_handle )
            gpx_file_data.append( gpx_data )

    #print("\tGPX file parsing complete")

    feet_in_one_meter = 3.28084

    print("\tGeotagging all images")
    for curr_file_name in source_file_manifests:
        #print( f"\t\tTrying to geocode {curr_file_name}")
        #print( "image data:\n" + json.dumps(source_file_manifests[curr_file_name], indent=4, sort_keys=True, default=str))

        curr_file_data = source_file_manifests[curr_file_name]
        curr_timestamp = curr_file_data['timestamp']
        for curr_gpx_data in gpx_file_data:
            computed_location = curr_gpx_data.get_location_at( curr_timestamp )
            if computed_location:
                location_info = {
                    'latitude_wgs84_degrees': computed_location[0].latitude,
                    'longitude_wgs84_degrees'             : computed_location[0].longitude,
                    'elevation_above_sea_level'     : {
                        'meters'    : computed_location[0].elevation,
                        'feet'      : computed_location[0].elevation * feet_in_one_meter,
                    },
                }

                curr_file_data['geotag'] = location_info
            else:
                #print( "\tlocation not found")
                pass

        if 'geotag' not in curr_file_data:
            print( f"\tINFO: no location was found in GPX files for {curr_file_name}" )

    time_end = time.perf_counter()
    operation_time_seconds = time_end - time_start

    #print("\tAll images geocoded" )

    return {
        'operation_time_seconds': operation_time_seconds,
    }


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
    file_data[ 'destination_xmp_file' ] = os.path.join( date_components['year'],
                                                       date_components['date_iso8601'],
                                                       f"{filename_no_extension}.xmp" )

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


def _xmp_creation_worker( worker_index, files_to_create_xmp_files_queue, parent_done_writing, program_options ):

    with exiftool.ExifTool(program_options['exiftool_path']) as exiftool_handle:

        # Create geotag parameters as those won't change
        geotag_parameters = []

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

            # Yeah, geotagging with Exiftool is a nonstarter. Just too much hassle. I could do it at commandline,
            #   But when trying to do with the pyexiftool wrapper it lacked GPS coords.  I give up.

            # Finally got what looks like a sane XMP with:
            # "c:\Program Files (x86)\ExifTool\exiftool.exe" -geotag utah.gpx "-geotime<${DateTimeOriginal}-08:00" 694A0001.CR3 -o %d%f.xmp

            # Had to do -8:00 because we took it in -0600 but I had the clock set two hours behind because 24 hour clock math is hard

            # Generated XMP
            # <?xpacket begin='﻿' id='W5M0MpCehiHzreSzNTczkc9d'?>
            # <x:xmpmeta xmlns:x='adobe:ns:meta/' x:xmptk='Image::ExifTool 12.30'>
            # <rdf:RDF xmlns:rdf='http://www.w3.org/1999/02/22-rdf-syntax-ns#'>
            #
            #  <rdf:Description rdf:about=''
            #   xmlns:exif='http://ns.adobe.com/exif/1.0/'>
            #   <exif:GPSAltitude>128147/100</exif:GPSAltitude>
            #   <exif:GPSAltitudeRef>0</exif:GPSAltitudeRef>
            #   <exif:GPSLatitude>41,3.772866N</exif:GPSLatitude>
            #   <exif:GPSLongitude>112,14.433294W</exif:GPSLongitude>
            #  </rdf:Description>
            # </rdf:RDF>
            # </x:xmpmeta>
            # <?xpacket end='w'?>

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

            #print( "Exiftool execute results:\n" + execute_output.decode() )

    #print( "Child exiting cleanly")


def _add_geotags_to_xmp( destination_file_manifests, program_options ):
    print( "\nAdding geotags to XMP files")

    start_time = time.perf_counter()

    for curr_year in sorted(destination_file_manifests):
        for curr_date in sorted(destination_file_manifests[curr_year]):
            for curr_file in sorted(destination_file_manifests[curr_year][curr_date]):
                curr_image_data = destination_file_manifests[curr_year][curr_date][curr_file]
                #print( f"\tAdding geotag for {curr_file}" )
                #print( "\tFile info from manifest for this file: " + json.dumps(curr_image_data, indent=4, sort_keys=True, default=str) )

                if 'geotag' in curr_image_data:
                    geotag_data = curr_image_data['geotag']

                    #print( "Going to insert geotag data into EXIF data of XMP file: " + \
                    #    json.dumps(geotag_data, sort_keys=True, indent=4, default=str) )

                    latitude_string = str( abs(geotag_data['latitude_wgs84_degrees']) )

                    # Spec for how GPS Coordinates should be formatted in EXIF portion of XMP:
                    #   https://www.cipa.jp/std/documents/e/DC-X010-2017.pdf
                    #
                    # A.2.4.4 GPSCoodinate
                    #
                    # Value type of GPSCoodinate is a Text value in the form “DDD,MM,SSk” or “DDD,MM.mmk”, where:
                    #
                    # • DDD is a number of degrees
                    # • MM is a number of minutes
                    # • SS is a number of seconds
                    # • mm is a fraction of minutes
                    # • k is a single character N, S, E, or W indicating a direction (north, south, east, west)
                    #
                    # Leading zeros are not necessary for the for DDD, MM, and SS values. The DDD,MM.mmk form should be used
                    # when any of the native Exif component rational values has a denominator other than 1. There can be any
                    # number of fractional digits.

                    latitude_degrees_string = str(abs(int(geotag_data['latitude_wgs84_degrees'])))
                    latitude_fractional_degrees = abs(geotag_data['latitude_wgs84_degrees']) % 1
                    latitude_fractional_minutes_string = f"{latitude_fractional_degrees * 60.0:.06f}"

                    if geotag_data['latitude_wgs84_degrees'] >= 0:
                        latitude_hemisphere = "N"
                    else:
                        latitude_hemisphere = "S"

                    latitude_string = f"{latitude_degrees_string},{latitude_fractional_minutes_string}{latitude_hemisphere}"

                    longitude_degrees_string = str(abs(int(geotag_data['longitude_wgs84_degrees'])))
                    longitude_fractional_degrees = abs(geotag_data['longitude_wgs84_degrees']) % 1
                    longitude_fractional_minutes_string = f"{longitude_fractional_degrees * 60.0:.06f}"

                    if geotag_data['longitude_wgs84_degrees'] >= 0:
                        longitude_hemisphere = "E"
                    else:
                        longitude_hemisphere = "W"

                    longitude_string = f"{longitude_degrees_string},{longitude_fractional_minutes_string}{longitude_hemisphere}"

                    string_replace_contents = \
                        f"  <exif:GPSAltitude>{int(geotag_data['elevation_above_sea_level']['meters'] * 100)}/100</exif:GPSAltitude>\n" \
                         "  <exif:GPSAltitudeRef>0</exif:GPSAltitudeRef>\n" \
                        f"  <exif:GPSLatitude>{latitude_string}</exif:GPSLatitude>\n" \
                        f"  <exif:GPSLongitude>{longitude_string}</exif:GPSLongitude>\n"

                    # should_look_like = \
                    #      "  <exif:GPSAltitude>128147/100</exif:GPSAltitude>\n" \
                    #      "  <exif:GPSAltitudeRef>0</exif:GPSAltitudeRef>\n" \
                    #      "  <exif:GPSLatitude>41,3.772866N</exif:GPSLatitude>\n" \
                    #      "  <exif:GPSLongitude>112,14.433294W</exif:GPSLongitude>\n"

                    # Read in the XMP file

                    xmp_file_path = os.path.join( program_options['laptop_destination_folder'],
                                                  curr_image_data['destination_xmp_file'] )

                    #print( f"\tXMP file: {xmp_file_path}")

                    with open( xmp_file_path, "r+") as xmp_handle:
                        xmp_contents = xmp_handle.read()

                        # Do replace
                        xmp_contents_with_geotag = xmp_contents.replace(
                            "  <exif:GPSAltitudeRef>0</exif:GPSAltitudeRef>\n",
                            string_replace_contents )

                        #print( f"XMP contents:\n{xmp_contents}")

                        # Seek back to start of the file
                        xmp_handle.seek(0)

                        # Write our new contents
                        xmp_handle.write( xmp_contents_with_geotag )

                        # Truncate the file
                        xmp_handle.truncate()

    end_time = time.perf_counter()
    operation_time_seconds = end_time - start_time

    print ("\tDone")

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

    program_options['sourcedirs'] = {
        'full'      : args.sourcedir_full,
        'partial'   : args.sourcedir_partial,
    }

    program_options['file_extension'] = args.fileext.lower()
    program_options['exiftool_path'] = args.exiftool
    program_options['file_timestamp_utc_offset_hours'] = args.file_timestamp_utc_offset_hours
    program_options['gpx_file_folder'] = args.gpx_file_folder
    program_options['laptop_destination_folder'] = args.laptop_destination_folder

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

    # Geocode all images now that we know their timestamps
    geocode_images_results = _geocode_images( program_options, source_file_manifest )
    perf_timer.add_perf_timing( 'Geocoding images', geocode_images_results['operation_time_seconds'])

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
    print("\nCreating XMP sidecar files for all RAW images")
    xmp_generation_results = _create_xmp_files( destination_file_manifests, program_options )
    print( "\tDone")
    perf_timer.add_perf_timing(  'XMP File Generation', xmp_generation_results['operation_time_seconds'])

    # Update XMP sidecar files with geotags
    add_geotags_to_xmp_results = _add_geotags_to_xmp( destination_file_manifests, program_options )
    perf_timer.add_perf_timing( 'Adding geotags to XMP files', add_geotags_to_xmp_results['operation_time_seconds'] )

    # Pull geotags out of XMP and store in manifest

    # Write manifest files to each date dir in scratch

    # TODO:
    #
    #   - Change sourcedir to be sourcedir_full (e.g., 512GB CFexpress type B) and sourcedir_partial (256 SD cards)
    #   - Add results of all partials into one hash. When done, compare to the full.
    #   - Parallelize reading source imagery (reading two 256 GB partial SD cards and 512 GB CFexpress can fit in 40 gigabits with no contention)
    #   - Add optional SD card destinations to arguments
    #   - One thread per SD output
    #       - Do all writes and then read back from external to make sure it worked

    # ZIP up the entire day in scratch storage (no compression!)
    # zipfile_path = os.path.join( "e:\\", "test.zip" )
    # print(f"\nCreating zip file {zipfile_path}")
    # zip_file = zipfile.ZipFile( zipfile_path, mode='w' )
    #
    # for  curr_year in sorted(destination_file_manifests):
    #     for  curr_date in sorted(destination_file_manifests[curr_year]):
    #         curr_manifest = destination_file_manifests[curr_year][curr_date]
    #         # print( f"Processing manifest for {curr_date_folder}" )
    #         for curr_manifest_filename in sorted(curr_manifest):
    #             curr_file_info = curr_manifest[curr_manifest_filename]
    #             #print( json.dumps( curr_file_info, indent=4, sort_keys=True, default=str) )
    #             file_to_add = os.path.join( scratch_write_tempdir.name, curr_year, curr_date, curr_manifest_filename )
    #             zip_file.write( file_to_add )
    #
    # print(f"\tZip file {zipfile_path} created")

    # Take hashes of each ZIP file

    # Do test unzip to scratch

    # Compare files extracted from ZIP match hashes in manifest

    # Do copy of known-good ZIP to each trip capacity storage device (e.g., SanDisk Extreme Pro V2 4TB SSD)

    # Check hash of ZIP file on each trip capacity storage device


    # Final perf print
    perf_timer.display_performance()

if __name__ == "__main__":
    _main()
