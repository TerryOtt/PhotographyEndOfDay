import threading

class PerformanceTimer:
    """Performance timing for an app. Thread safe (note lock)"""

    # Singleton pattern totally stolen from
    #       https://python-3-patterns-idioms-test.readthedocs.io/en/latest/Singleton.html
    class __PerformanceTimer:

        def __init__(self):
            self._perf_data = {
                'total': 0.0,
                'entries': [],
            }

        def __str__(self):
            return repr(self) + self.__perf_data


        def add_perf_timing(self, label, value):
            self._perf_data['entries'].append(
                {
                    'label': label,
                    'value': value,
                }
            )
            self._perf_data['total'] += value


        def display_performance(self):
            # Find longest label
            longest_label_len = len('Total')
            for entry in self._perf_data['entries']:
                if len(entry['label']) > longest_label_len:
                    longest_label_len = len(entry['label'])

            print("\nProfiling stats:\n")

            for curr_entry in self._perf_data['entries']:
                percentage_time = (curr_entry['value'] / self._perf_data['total']) * 100.0
                print(f"\t{curr_entry['label']:>{longest_label_len}s} : {curr_entry['value']:>7.03f} seconds " +
                      f"({percentage_time:5.01f}%)")

            total_label = "Total"
            print(f"\n\t{total_label:>{longest_label_len}s} : {self._perf_data['total']:>7.03f} seconds")

    __singleton_instance = None
    __singleton_lock = threading.Lock()

    def __init__(self):
        with PerformanceTimer.__singleton_lock:
            if not PerformanceTimer.__singleton_instance:
                PerformanceTimer.__singleton_instance = PerformanceTimer.__PerformanceTimer()

    def __getattr__(self, name):
        with PerformanceTimer.__singleton_lock:
            return getattr(self.__singleton_instance, name)