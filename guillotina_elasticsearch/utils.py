

class NoopResponse:
    def write(self, *args, **kwargs):
        pass


noop_response = NoopResponse()
