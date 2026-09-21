from mrjob.job import MRJob
from mrjob.step import MRStep
import json

class MRContactSubscriptionByMonth(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_month_subscription,
                   reducer=self.reducer_count_contacts_subscriptions)
        ]

    def mapper_get_month_subscription(self, _, line):
        # Skip the header row
        if line.startswith('age'):
            return
        fields = line.split(',')
        # Assuming month is at index 10 and subscription status 'y' is at index 16
        try:
            month = fields[10]
            subscription_status = fields[16]
            yield month, subscription_status
        except IndexError:
            # Handle potential errors in data format
            pass

    def reducer_count_contacts_subscriptions(self, month, statuses):
        total_contacts = 0
        subscribed_clients = 0
        for status in statuses:
            total_contacts += 1
            if status == 'yes':
                subscribed_clients += 1
        # Yield the month and counts, formatted as a JSON string
        yield month, json.dumps({'total_contacts': total_contacts, 'subscribed_clients': subscribed_clients})

if __name__ == '__main__':
    MRContactSubscriptionByMonth.run()
