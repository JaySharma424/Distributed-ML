from mrjob.job import MRJob
from mrjob.step import MRStep
import json

class MRAgeBalanceRelationship(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_age_balance,
                   reducer=self.reducer_calculate_average_balance)
        ]

    def mapper_get_age_balance(self, _, line):
        # Skip the header row
        if line.startswith('age'):
            return
        fields = line.split(',')
        # Assuming age is at index 0 and balance is at index 5
        try:
            age = int(fields[0])
            balance = int(fields[5])
            yield age, balance
        except (IndexError, ValueError):
            # Handle potential errors in data format or type conversion
            pass


    def reducer_calculate_average_balance(self, age, balances):
        total_balance = 0
        count = 0
        for balance in balances:
            total_balance += balance
            count += 1
        if count > 0:
            average_balance = total_balance / count
            # Yield the age and average balance, formatted as a JSON string
            yield age, json.dumps(average_balance)

if __name__ == '__main__':
    MRAgeBalanceRelationship.run()
