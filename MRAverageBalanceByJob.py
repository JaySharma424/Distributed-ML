from mrjob.job import MRJob
from mrjob.step import MRStep
import json

class MRAverageBalanceByJob(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_balance,
                   reducer=self.reducer_calculate_average)
        ]

    def mapper_get_balance(self, _, line):
        # Skip the header row
        if line.startswith('age'):
            return
        fields = line.split(',')
        # Assuming job is at index 1 and balance is at index 5
        try:
            job = fields[1]
            balance = int(fields[5])
            yield job, balance
        except (IndexError, ValueError):
            # Handle potential errors in data format or type conversion
            pass

    def reducer_calculate_average(self, job, balances):
        total_balance = 0
        count = 0
        for balance in balances:
            total_balance += balance
            count += 1
        if count > 0:
            average_balance = total_balance / count
            # Yield the job and average balance, formatted as a JSON string
            yield job, json.dumps(average_balance)

if __name__ == '__main__':
    MRAverageBalanceByJob.run()
