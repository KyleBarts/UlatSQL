import time
from locust import HttpUser, task, between

class QuickstartUser(HttpUser):
    wait_time = between(1, 2.5)

    @task
    def vpoteka_filtered1day(self):
        self.client.get("/vpoteka?start_date=2020-05-27 18:30:00&end_date=2020-06-27 18:29:59")

    @task
    def vpoteka_filtered12hr(self):
        self.client.get("/vpoteka?start_date=2020-05-27 18:30:00&end_date=2020-06-27 6:30:30")

    @task
    def vpoteka_filtered6hr(self):
        self.client.get("/vpoteka?start_date=2020-05-27 18:30:00&end_date=2020-05-27 23:29:59")

    @task
    def vpoteka_filtered3hr(self):
        self.client.get("/vpoteka?start_date=2020-05-27 18:30:00&end_date=2020-05-27 21:29:59")

    @task
    def vpoteka_filtered1hr(self):
        self.client.get("/vpoteka?start_date=2020-05-27 18:30:00&end_date=2020-05-27 19:29:59")

