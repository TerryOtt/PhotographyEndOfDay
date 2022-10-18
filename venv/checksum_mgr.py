import logging
import multiprocessing
import checksum_worker
import json


class ChecksumManager:
    def __init__(self, total_number_of_files_to_checksum, console_display_messages_queue,
                 checksum_queue_length, number_of_worker_processes_allowed ):

        max_size_update_queue = checksum_queue_length
        self.checksum_update_queue = multiprocessing.Queue( max_size_update_queue )
        (self.checksums_computed_pipe_read_connection,
         self.checksums_computed_pipe_write_connection) = multiprocessing.Pipe(duplex=False)
        self.total_number_of_files_to_checksum = total_number_of_files_to_checksum
        self.console_display_messages_queue = console_display_messages_queue
        self.number_of_worker_processes_allowed = number_of_worker_processes_allowed

        self.coordinator_process_handle = multiprocessing.Process(target=checksum_worker.checksum_coordinator,
                                          args=(self.total_number_of_files_to_checksum,
                                                self.checksum_update_queue,
                                                self.checksums_computed_pipe_write_connection,
                                                self.console_display_messages_queue,
                                                self.number_of_worker_processes_allowed) )

        self.coordinator_process_handle.start()


    def validate_all_checksums_match(self, number_unique_files, number_of_copies_of_each_unique_file):
        #print("\n\nChecksum manager requested to validate all checksums, waiting for write to pipe")

        checksum_data_msg = self.checksums_computed_pipe_read_connection.recv()

        print( "\nValidating all computed checksums")

        # Validate  all checksums match and throw a damn fit if not
        #print( json.dumps(checksum_data_msg, indent=4, sort_keys=True, default=str) )

        validation_passed = True

        # Make sure the right number of unique files
        if len(checksum_data_msg) != number_unique_files:
            print( f"\tERROR: Number of unique files checksummed ({len(checksum_data_msg)}) does not equal " +
                   f"number of unique files ({number_unique_files})")
            validation_passed = False
        else:
            print( f"\tNumber of unique files checksummed is correct ({number_unique_files})")

        # For each unique file
        for curr_relative_file in checksum_data_msg:

            # Make sure only one checksum found across all the copies of that file
            num_checksums_for_this_file = len( checksum_data_msg[curr_relative_file])
            if num_checksums_for_this_file != 1:
                print( f"\tERROR: not all copies of file {curr_relative_file} have the same checksum!" )
                print( curr_relative_file + ": " + json.dumps(checksum_data_msg[curr_relative_file],
                                                              indent=4, sort_keys=True) )
                validation_passed = False
            else:
                correct_checksum = list(checksum_data_msg[curr_relative_file].keys())[ 0 ]
                curr_relative_file_copies = checksum_data_msg[curr_relative_file][correct_checksum]

                # Make sure right number of copies
                if len(curr_relative_file_copies) != number_of_copies_of_each_unique_file:
                    print( f"\tERROR: did not get correct number of copies for {curr_relative_file}" )
                    validation_passed = False

        if validation_passed is True:
            total_copies_checked = number_unique_files * number_of_copies_of_each_unique_file
            print( f"\tTotal number of copies that passed checksums is correct ({total_copies_checked})")
            print( "\tAll validation checks passed successfully!" )
        else:
            print( "\tSome validation tests failed")

        self.coordinator_process_handle.join()
        #print( "\nChecksum mgr had checksum coordinator process rejoin cleanly")


