from millegrilles_filehost.Context import FileHostContext, StopListener


class WebServer(StopListener):

    def __init__(self, context: FileHostContext):
        super().__init__()
        self.__context = context

    async def stop(self):
        pass

    async def run(self):
        print("Running")
        pass