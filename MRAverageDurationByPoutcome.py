from mrjob.job import MRJob
from mrjob.step import MRStep
import json

class MRAverageDurationByPoutcome(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_duration_poutcome,
                   reducer=self.reducer_calculate_average_duration)
        ]

    def mapper_get_duration_poutcome(self, _, line):
        # Skip the header row
        if line.startswith('age'):
            return
        fields = line.split(',')
        # Assuming duration is at index 11 and poutcome is at index 15
        try:
            duration = int(fields[11])
            poutcome = fields[15]
            yield poutcome, duration
        except (IndexError, ValueError):
            # Handle potential errors in data format or type conversion
            pass


    def reducer_calculate_average_duration(self, poutcome, durations):
        total_duration = 0
        count = 0
        for duration in durations:
            total_duration += duration
            count += 1
        if count > 0:
            average_duration = total_duration / count
            # Yield the poutcome and average duration, formatted as a JSON string
            yield poutcome, json.dumps(average_duration)

if __name__ == '__main__':
    MRAverageDurationByPoutcome.run()
