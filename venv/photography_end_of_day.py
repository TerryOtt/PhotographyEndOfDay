import argparse
import multiprocessing
import logging
import json
import os
import sys
import exiftool             # Requires pip3 install pyexiftool >= 0.5
import datetime

curr_processes_running = 1

def _parse_args():
    arg_parser = argparse.ArgumentParser(description="End of day script to create validated travel copies of all pics")
    arg_parser.add_argument("--debug", help="Lower logging level from INFO to DEBUG", action="store_true")
    arg_parser.add_argument("--logfile",
                            help="file to store logs to (default: stdout)" )
    arg_parser.add_argument("--singlethreaded", help="Disable multi-threading, do everything single threaded",
                            action="store_true")

    # The "Action append" lets the parameter be specified multiple times, but it's still optional so may exist no times
    arg_parser.add_argument("--sourcedir",
                            help="Path to a source directory with RAW files",
                            required=True,
                            action='append')

    arg_parser.add_argument("--timestamp_utc_offset_hours",
                            help="Integer hours offset from UTC, e.g., EDT is -4, PDT is -7",
                            type=int,
                            default=0)

    max_processes_default = multiprocessing.cpu_count() - 1
    arg_parser.add_argument("--max_processes",
                            help="Maximum number of processes running at one time (used to limit worker processes)" +
                                f" (default on this computer: {max_processes_default})",
                            default=max_processes_default,
                            type=int )

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
    total_file_bytes = 0

    exiftool_tag_name = "EXIF:DateTimeOriginal"

    # Create a dictionary that maps all absolute file paths to the relative one they have in common
    for curr_sourcedir in program_options['sourcedirs']:
        source_file_lists[curr_sourcedir] = _scan_source_dir_for_images(curr_sourcedir, program_options )

    # Populate EXIF timestamps for all images
    raw_file_list = []
    reverse_map = {}
    for curr_sourcedir in source_file_lists:
        for (index, curr_file_entry) in enumerate(source_file_lists[curr_sourcedir]):
            raw_file_list.append(curr_file_entry['file_path']['absolute'])

            # Store a reference into the source array keyed by absolute filename so we can do a quick update with the timestamp
            reverse_map[ curr_file_entry['file_path']['absolute'] ] = source_file_lists[curr_sourcedir][index]

    #print( f"Reverse map created:\n{json.dumps(reverse_map, indent=4, sort_keys=True)}")

    raw_file_list.sort()

    with exiftool.ExifToolHelper() as exiftool_handle:
        exiftool_tag_name = "EXIF:DateTimeOriginal"
        file_timestamps = exiftool_handle.get_tags( raw_file_list, tags = [ exiftool_tag_name] )

    #print( f"Timestamp results:\n{json.dumps(file_timestamps, indent=4, sort_keys=True)}")

    for curr_timestamp_entry in file_timestamps:
        file_datetime_no_tz = datetime.datetime.strptime(curr_timestamp_entry[exiftool_tag_name], "%Y:%m:%d %H:%M:%S")
        # Do hour shift from timezone-unaware EXIF datetime to UTC (still no TZ, just shifting hours)
        shifted_datetime_no_tz = file_datetime_no_tz + datetime.timedelta(
            hours=-(program_options['timestamp_utc_offset_hours']))
        # Create TZ-aware datetime, as one should basically always strive to use
        file_datetime_utc = shifted_datetime_no_tz.replace(tzinfo=datetime.timezone.utc)

        # Convert the SourceFile tag to be windows-friendly (backslashes)
        reverse_map_key = curr_timestamp_entry['SourceFile'].replace( '/', os.sep)
        reverse_map[ reverse_map_key ]['timestamp'] = file_datetime_utc

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
    for curr_file_entry in source_file_lists[program_options['sourcedirs'][0]]:
        file_dict[ curr_file_entry['file_path']['relative']] = {
            'filesize_bytes'    : curr_file_entry['filesize_bytes'],
            'timestamp'         : curr_file_entry['timestamp']
        }
        total_file_bytes += curr_file_entry['filesize_bytes']

    bytes_in_gb = 1073741824.0
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


def _main():
    args = _parse_args()
    program_options = {}

    if args.debug is False:
        log_level = logging.INFO
    else:
        log_level = logging.DEBUG

    if args.logfile:
        logging.basicConfig( filename=args.logfile, level=log_level )
    else:
        logging.basicConfig(level=log_level)

    if args.sourcedir:
        program_options['sourcedirs'] = sorted(args.sourcedir)
    else:
        program_options['sourcedirs'] = []

    program_options['file_extension'] = args.raw_file_fileext.lower()
    program_options['timestamp_utc_offset_hours'] = args.timestamp_utc_offset_hours
    program_options['destination_folders'] = args.travel_storage_media_folder

    logging.debug( f"Program options: {json.dumps(program_options, indent=4, sort_keys=True)}" )

    source_image_info = _enumerate_source_images(program_options)
    #print( "\nSource file info:\n" + json.dumps(source_image_info['source_file_dict'],
    #                                                             indent=4, sort_keys=True, default=str) )
    # Determine unique filenames
    set_destination_filenames_results = _set_destination_filenames( program_options, source_image_info['source_file_dict'] )
#
#     # Do file copies to laptop NVMe SSD
#     copy_operation_results = _do_file_copies_to_laptop(program_options, source_file_manifest)
#     perf_timer.add_perf_timing( 'Copying RAW files to laptop NVMe',
#                      copy_operation_results['operation_time_seconds'])
#
#     # Do readback validation to make sure all writes to laptop worked
#     print("\nReading files back from laptop SSD to verify contents still match original hash")
#     verify_operation_results = _do_readback_validation( source_file_manifest, program_options )
#     print( "\tDone")
#     perf_timer.add_perf_timing( 'Validating all writes to laptop NVMe SSD are still byte-identical to source',
#         verify_operation_results['operation_time_seconds'])
#
#     # Create XMP sidecar files
# #    print("\nCreating geotagged XMP sidecar files for all RAW images")
# #    xmp_generation_results = _create_xmp_files( destination_file_manifests, program_options )
# #    print( "\tDone")
# #    perf_timer.add_perf_timing(  'Generating geotagged XMP files', xmp_generation_results['operation_time_seconds'])
#
#     # Pull geotags out of XMP and store in manifest
# #    print( "\nUpdating manifest with geotag and XMP hash data")
# #    geotag_and_timestamp_manifest_update_results = _update_manifest_with_geotags( program_options,
# #                                                                                  destination_file_manifests )
# #    print( "\tDone" )
# #    perf_timer.add_perf_timing( "Adding geotags and XMP file hashes to manifest", geotag_and_timestamp_manifest_update_results['operation_time_seconds'])
#
#     # Create (or update) daily manifest files
#     print( "\nWriting or updating per-day manifest files" )
#     manifest_write_results = _write_manifest_files( program_options, destination_file_manifests )
#     print( "\tDone")
#     perf_timer.add_perf_timing( "Writing per-day manifest files to disk",
#                                 manifest_write_results['operation_time_seconds'] )
#
#     # Copy from laptop to external storage
#     external_copies_time_seconds = _copy_files_to_external_storage( program_options, source_file_manifest )
#     perf_timer.add_perf_timing( 'Copying all files from laptop to all travel storage media devices',
#                                  external_copies_time_seconds )
#
#     # Validate external storage copies
#     print( "\nVerifying all travel media copies")
#     travel_media_verify_time_seconds = _verify_travel_media_copies( program_options, destination_file_manifests )
#     print( "\tDone")
#     perf_timer.add_perf_timing( "Verifying all travel media copies match original hashes",
#                                 travel_media_verify_time_seconds )
#
#     # Final perf print
#     print( "" )
#     perf_timer.display_performance()


if __name__ == "__main__":
    _main()