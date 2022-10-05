import argparse
import multiprocessing
import logging
import json

# import pprint
# import json
# import logging
# import time
# import os
# import hashlib
# import multiprocessing
# import queue
# import exiftool
# import datetime
# import shutil
# import pathlib
# import glob
# import performance_timer
# import random
# import shutil
# import xml.etree.ElementTree
# import copy


#num_worker_processes = multiprocessing.cpu_count() - 2
# max_processes_running = 15
# curr_processes_running = 1

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

    arg_parser.add_argument("raw_file_fileext", help="File extension, e.g. \"NEF\", \"CR3\"")

    arg_parser.add_argument("travel_storage_media_folder", nargs="+",
                            help="Travel storage folder " + \
                                 "(e.g., laptop NVMe drive, SanDisk Extreme Pro 4TB, WD My Passport 4TB)")

    return arg_parser.parse_args()

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
#
#     perf_timer = performance_timer.PerformanceTimer()
#
#     source_image_info = _enumerate_source_images(program_options)
#     perf_timer.add_perf_timing( 'Enumerating source images', source_image_info['operation_time_seconds'])
#
#     #print( "File list to create source manifest:\n" + json.dumps(source_image_info['file_list'], indent=4, sort_keys=True) )
#     #print( "Reverse map for hashing:\n" + json.dumps(source_image_info['reverse_map'], indent=4) )
#
#     manifest_info = _generate_source_manifest( source_image_info['reverse_map'],
#                                                source_image_info['source_file_list'] )
#     # Delete the reverse map, don't need it anymore
#     del source_image_info['reverse_map']
#     perf_timer.add_perf_timing(  'Creating source image manifest', manifest_info['operation_time_seconds'])
#     source_file_manifest = manifest_info['source_manifest']
#
#     # Get timestamp for all image files
#     timestamp_output = _extract_image_timestamps( program_options, source_file_manifest )
#     perf_timer.add_perf_timing( 'Extracting EXIF timestamps', timestamp_output['operation_time_seconds'])
#
#     # Enumerate files already in the destination directory
#     destination_files_results = _get_existing_files_in_destination( source_file_manifest, program_options )
#     perf_timer.add_perf_timing( "Listing existing files in destination folder",
#                                 destination_files_results['operation_time_seconds'] )
#     existing_destination_files = destination_files_results['existing_files']
#
#     # Determine unique filenames
#     set_destination_filenames_results = _set_destination_filenames( program_options, source_file_manifest,
#                                                                     existing_destination_files )
#     perf_timer.add_perf_timing( 'Generating unique destination filenames',
#                       set_destination_filenames_results['operation_time_seconds'] )
#     destination_file_manifests = set_destination_filenames_results['destination_file_manifests']
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