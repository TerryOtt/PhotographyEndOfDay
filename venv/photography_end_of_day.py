import pprint
import json
import logging
import argparse
import time
import os
import hashlib
import multiprocessing
import queue


def _parse_args():
    arg_parser = argparse.ArgumentParser(description="End of day script to validate data, geotag, copy to SSD")
    arg_parser.add_argument("--debug", help="Lower logging level from INFO to DEBUG", action="store_true" )
    arg_parser.add_argument("--logfile", help="Path to file where the app log will be created",
                            default="photo_end_of_day.log" )
    arg_parser.add_argument( "--sourcedir", help="Path to a source directory", required=True, action="append" )
    arg_parser.add_argument( "--fileext", help="File extension, e.g. \"NEF\", \"CR3\"", required=True )
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

    for i in range(1): # multiprocessing.cpu_count()):
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

    logging.debug( f"Program options: {json.dumps(program_options, indent=4, sort_keys=True)}" )

    perf_timings = {
        'total' : 0.0,
        'entries': [],
    }

    source_image_info = _enumerate_source_images(program_options)
    _add_perf_timing(perf_timings, 'Enumerating source images', source_image_info['operation_time_seconds'])

    manifest_info = _generate_source_manifest(source_image_info['source_dirs'])
    _add_perf_timing(perf_timings, 'Creating source image manifest', manifest_info['operation_time_seconds'])
    #logging.debug( json.dumps( manifest_info['source_manifest'], sort_keys=True, indent=4) )

    # If there are two source dirs, they have to be identical

    #logging.debug( json.dumps(source_image_info, indent=4, sort_keys=True))

    # Final perf print
    _display_perf_timings( perf_timings )


if __name__ == "__main__":
    _main()
