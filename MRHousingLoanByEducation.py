from mrjob.job import MRJob
from mrjob.step import MRStep
import json

class MRHousingLoanByEducation(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_loan_status,
                   reducer=self.reducer_count_loan_status)
        ]

    def mapper_get_loan_status(self, _, line):
        # Skip the header row
        if line.startswith('age'):
            return
        fields = line.split(',')
        # Assuming education is at index 3 and housing is at index 6
        try:
            education = fields[3]
            housing_loan = fields[6]
            yield education, housing_loan
        except IndexError:
            # Handle potential errors in data format
            pass

    def reducer_count_loan_status(self, education, housing_loans):
        loan_counts = {'yes': 0, 'no': 0}
        for status in housing_loans:
            if status in loan_counts:
                loan_counts[status] += 1
        # Yield the education and loan counts, formatted as a JSON string
        yield education, json.dumps(loan_counts)

if __name__ == '__main__':
    MRHousingLoanByEducation.run()
