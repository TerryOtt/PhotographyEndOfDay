import logging
import multiprocessing
import checksum_worker
import json


class ChecksumManager:
    def __init__(self, total_number_of_files_to_checksum, console_display_messages_queue,
                 number_of_worker_processes_allowed ):

        max_size_update_queue = 1000
        self.checksum_update_queue = multiprocessing.Queue( max_size_update_queue )
        (self.checksums_computed_pipe_read_connection,
         self.checksums_computed_pipe_write_connection) = multiprocessing.Pipe(duplex=False)
        self.console_display_messages_queue = console_display_messages_queue
        self.number_of_worker_processes_allowed = number_of_worker_processes_allowed

        self.coordinator_process_handle = multiprocessing.Process(target=checksum_worker.checksum_coordinator,
                                          args=(total_number_of_files_to_checksum,
                                                self.checksum_update_queue,
                                                self.checksums_computed_pipe_write_connection,
                                                self.console_display_messages_queue,
                                                self.number_of_worker_processes_allowed) )

        self.coordinator_process_handle.start()


    def validate_all_checksums_match(self):
        print("Checksum manager requested to validate all checksums, waiting for write to pipe")

        checksum_data_msg = self.checksums_computed_pipe_read_connection.recv()

        print( "Checksum manager got list of all computed checksums from checksum coordinator!")

        # TODO: validate all checksums match and throw a damn fit if not
        print( "Checksums received from checksum coordinator:\n" +
               json.dumps(checksum_data_msg, indent=4, sort_keys=True, default=str))

        print( "All checksums match (maybe? Didn't really do any work), checksum mgr waiting for process rejoin")
        self.coordinator_process_handle.join()
        print( "Checksum mgr had checksum coordinator process rejoin cleanly")


