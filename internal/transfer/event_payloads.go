package transfer

const moduleEventPrefix = "transfer"

func eventName(name string) string {
	return moduleEventPrefix + "." + name
}

const RequestTransferAcceptanceEvtName = moduleEventPrefix + "." + "requestTransferAcceptance"

type RequestTransferAcceptancePayload struct {
	TransferId TransferId
}

const RequestDownloadDestinationEvtName = moduleEventPrefix + "." + "requestDownloadDestination"

type RequestDownloadDestinationPayload struct {
	TransferId TransferId
}

const TransferTimedOutEvtName = moduleEventPrefix + "." + "timedOut"

type TransferTimedOutPayload struct {
	TransferId TransferId
	Message    string
}
