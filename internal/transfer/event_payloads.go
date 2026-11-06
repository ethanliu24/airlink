package transfer

const moduleEventPrefix = "transfer"

func eventName(name string) string {
	return moduleEventPrefix + "." + name
}

const RequestTransferAcceptanceEvtName = moduleEventPrefix + "." + "requestTransferAcceptance"
type RequestTransferAcceptancePayload struct {
	TransferId TransferId
}
