import hashlib
import logging
import json
import multiprocessing
import queue
import time


def checksum_coordinator(total_number_of_files_to_checksum,
                         checksum_update_queue,
                         all_checksums_computed_write_connection,
                         console_display_messages_queue,
                         number_of_worker_processes_allowed):

    coordinator_starting_msg = {
        "msg_level": logging.INFO,
        "msg": f"Checksum coordinator started, expecting {total_number_of_files_to_checksum} files for checksum",
    }
    console_display_messages_queue.put( coordinator_starting_msg )

    checksums_computed = 0
    completed_checksums = {}
    worker_assignments = {}
    next_checksum_worker_to_assign = 0

    checksums_completed_queue = multiprocessing.Queue()

    checksum_worker_processes = _create_checksum_worker_processes( number_of_worker_processes_allowed,
                                                                   checksums_completed_queue )
    _start_checksum_worker_processes( checksum_worker_processes )

    checksum_children_started_msg = {
        "msg_level" : logging.INFO,
        "msg"       : f"Checksum coordinator launched {len(checksum_worker_processes)} checksum processes",
    }
    console_display_messages_queue.put( checksum_children_started_msg )

    checksum_queue_blocking = False

    start_time = time.perf_counter()

    while checksums_computed < total_number_of_files_to_checksum:
        # See if any new work to dole out
        try:
            checksum_update_msg = checksum_update_queue.get( checksum_queue_blocking )
            #print( "Checksum coordinator saw new request:\n" + json.dumps(checksum_update_msg['file_info'], indent=4, sort_keys=True))

            file_absolute_path = checksum_update_msg['file_info']['absolute_path']
            file_relative_path = checksum_update_msg['file_info']['relative_path']

            if 'data_block' not in checksum_update_msg or ('data_block' in checksum_update_msg and
                                                           checksum_update_msg['data_block']['block_byte_start'] == 1):

                # Assign the checksum worker process for this new file
                worker_assignments[ file_absolute_path ] = next_checksum_worker_to_assign
                next_checksum_worker_to_assign = ( next_checksum_worker_to_assign + 1 ) % number_of_worker_processes_allowed

                # new_file_seen_msg = {
                #     "msg_level" : logging.DEBUG,
                #     "msg"       : f"Checksum coordinator saw new file \"{file_absolute_path}\"" + \
                #         ", file is assigned to checksum worker " + str(worker_assignments[ file_absolute_path ]),
                # }
                # console_display_messages_queue.put( new_file_seen_msg )

            # if 'data_block' not in checksum_update_msg or ('data_block' in checksum_update_msg and
            #                                                checksum_update_msg['data_block']['block_byte_end'] ==
            #                                                checksum_update_msg['file_info']['total_file_size']):
            #     last_chunk_being_sent_msg = {
            #         "msg_level": logging.DEBUG,
            #         "msg": f"Checksum coordinator sending last chunk of \"{file_absolute_path}\"" + \
            #                " to worker " + str(worker_assignments[file_absolute_path]),
            #     }
            #     console_display_messages_queue.put( last_chunk_being_sent_msg )

            # Pass the work chunk of work to the assigned child worker
            child_worker_msg = {
                'checksum_work': checksum_update_msg
            }
            checksum_worker_processes[ worker_assignments[file_absolute_path] ]['child_queue'].put(
                child_worker_msg
            )

            # Explicitly drop handle to hopefully drop memory sooner
            checksum_update_msg = None
        except queue.Empty:
            pass

        # Read any checksums computed by children
        try:
            while True:
                computed_checksum_msg = checksums_completed_queue.get( checksum_queue_blocking )

                checksum_relative_path  = computed_checksum_msg['relative_path']
                checksum_absolute_path  = computed_checksum_msg['absolute_path']
                checksum_value          = computed_checksum_msg['computed_checksum']

                computed_checksum_msg = None

                if checksum_relative_path not in completed_checksums:
                    completed_checksums[checksum_relative_path] = {}

                if checksum_value not in completed_checksums[checksum_relative_path]:
                    completed_checksums[checksum_relative_path][checksum_value] = []

                completed_checksums[checksum_relative_path][checksum_value].append( checksum_absolute_path )

                # computed_checksum_msg = {
                #     "msg_level": logging.INFO,
                #     "msg": f"Checksum coordinator got completed checksum msg:\n" + json.dumps(
                #         computed_checksum_msg, indent=4, sort_keys=True )
                # }
                # console_display_messages_queue.put(computed_checksum_msg)

                checksums_computed += 1

                #print( f"Checksums complete: {checksums_computed} / {total_number_of_files_to_checksum}")

        except queue.Empty:
            pass

    end_time = time.perf_counter()
    checksum_operation_time_seconds = end_time - start_time
    checksum_file_velocity = total_number_of_files_to_checksum // checksum_operation_time_seconds

    print( f"\nChecksums completed for {total_number_of_files_to_checksum:,} files in " +
               f"{checksum_operation_time_seconds:.03f} seconds ({checksum_file_velocity:,} files/sec)" )

    #print( "Coordinator thinks all checksums are done")

    # Blast the list of computed checksums back over the pipe so the checksum manager can read it and act
    all_checksums_computed_write_connection.send( completed_checksums )

    # Now send the sentinel value to all children that they can terminate cleanly
    terminate_msg = {
        'done_writing': True,
    }
    for curr_child_entry in checksum_worker_processes:
        curr_child_entry['child_queue'].put( terminate_msg )

    # Land all child worker processes now that they'll be coming home
    while checksum_worker_processes:
        curr_process_to_join = checksum_worker_processes.pop()
        curr_process_to_join['process_handle'].join()

    # all_children_rejoined_msg = {
    #     "msg_level": logging.INFO,
    #     "msg": f"Checksum coordinator had all child checksum worker processes rejoin, exiting cleanly",
    # }
    # console_display_messages_queue.put(all_children_rejoined_msg)


def _create_checksum_worker_processes( number_of_worker_processes_allowed, checksums_completed_queue ):
    checksum_worker_info = []

    # Dynamically compute this based on queue size passed in
    max_child_queue_length = 1000 // number_of_worker_processes_allowed

    for curr_checksum_worker_index in range(number_of_worker_processes_allowed):
        child_worker_queue = multiprocessing.Queue( max_child_queue_length )
        curr_worker_info = {
            'process_handle'        : multiprocessing.Process(target=_checksum_worker,
                                                              args=(curr_checksum_worker_index,
                                                                    child_worker_queue,
                                                                    checksums_completed_queue) ),
            'child_queue'    : child_worker_queue,
        }

        checksum_worker_info.append( curr_worker_info )

    return checksum_worker_info


def _start_checksum_worker_processes( checksum_worker_processes ):
    for i in range(len(checksum_worker_processes)):
        checksum_worker_processes[i]['process_handle'].start()

def _checksum_worker(child_worker_index, files_to_checksum_queue, checksums_completed_queue):
    hashlib_handles = {}

    work_remains = True
    while work_remains is True:
        msg_from_queue = files_to_checksum_queue.get()

        if 'done_writing' not in msg_from_queue:
            checksum_work_msg = msg_from_queue['checksum_work']
            file_absolute_path = checksum_work_msg['file_info']['absolute_path']
            file_relative_path = checksum_work_msg['file_info']['relative_path']

            # Do we have a hash handle for this file?
            if file_absolute_path not in hashlib_handles:
                hashlib_handles[ file_absolute_path ] = hashlib.sha3_512()


            # print( f"Worker {child_worker_index} got checksum data for " +
            #        json.dumps(checksum_work_msg['file_info'], indent=4, sort_keys=True))

            checksum_finished = False
            if 'data_block' not in checksum_work_msg:
                # Do entire file checksum
                with open( file_absolute_path, 'rb' ) as file_handle:
                    file_bytes = file_handle.read()

                hashlib_handles[ file_absolute_path ].update(file_bytes)

                # Explicitly drop reference to the memory storing the full file contents so it gets garbage collected sooner
                file_bytes = None

                checksum_finished = True
            else:
                # Do partial update
                hashlib_handles[file_absolute_path].update(checksum_work_msg['data_block']['block_payload'])

                # Delete the payload as it's the big part of the message
                del checksum_work_msg['data_block']['block_payload']

                if checksum_work_msg['data_block']['block_byte_end'] == \
                        checksum_work_msg['file_info']['total_file_size']:

                    checksum_finished = True

            if checksum_finished is True:
                computed_checksum_msg = {
                    'relative_path'     : file_relative_path,
                    'absolute_path'     : file_absolute_path,
                    'computed_checksum' : hashlib_handles[ file_absolute_path].hexdigest(),
                }

                checksums_completed_queue.put( computed_checksum_msg )

                computed_checksum_msg = None

                # We aren't going to use this hash handle again
                del hashlib_handles[file_absolute_path]

            checksum_work_msg = None
            msg_from_queue = None

        else:
            work_remains = False