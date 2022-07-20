def getDocumentHandler():
    loader = AnalysisLoader()

    def _handler(name, doc):
        if name == "stop":
            # run_analysis(stop)
            run = getRunFromStop(doc)
            run_analysis(run, loader)
        else:
            print(name)
    return _handler


def dispatch():
    d = RemoteDispatcher('localhost:5578')
    d.subscribe(getDocumentHandler())
    print("Ready for documents, starting handler")
    d.start()


if __name__ == "__main__":
    dispatch()
