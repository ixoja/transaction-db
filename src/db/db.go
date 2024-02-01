package db

type (
	actionType int

	action struct {
		aType actionType
		key   string
		value string
	}

	transaction struct {
		actions []*action
	}

	InMemoryDB struct {
		storage      map[string]string
		transactions map[int]*transaction
	}
)

const (
	ActionSet    actionType = 0
	ActionDelete actionType = 1
)

func New() *InMemoryDB {
	return &InMemoryDB{
		storage:      make(map[string]string),
		transactions: make(map[int]*transaction),
	}
}

func (d *InMemoryDB) Get(key string) string {
	return d.storage[key]
}

func (d *InMemoryDB) Set(key, value string) {
	// no active transactions, proceed
	if d.transactions[0] == nil {
		d.storage[key] = value
		return
	}

	tID := d.getCurrentTransactionID()
	d.transactions[tID].actions = append(d.transactions[tID].actions, &action{
		aType: ActionSet,
		key:   key,
		value: value,
	})
}

func (d *InMemoryDB) Delete(key string) {
	// no active transactions, proceed
	if d.transactions[0] == nil {
		delete(d.storage, key)
		return
	}

	tID := d.getCurrentTransactionID()
	d.transactions[tID].actions = append(d.transactions[tID].actions, &action{
		aType: ActionDelete,
		key:   key,
	})
}

func (d *InMemoryDB) StartTransaction() {
	if d.transactions[0] == nil {
		d.transactions[0] = &transaction{
			actions: make([]*action, 0, 5),
		}
		return
	}

	d.transactions[d.getCurrentTransactionID()+1] = &transaction{
		actions: make([]*action, 0, 5),
	}
	return
}

func (d *InMemoryDB) Commit() {
	// no active transactions, skip
	if d.transactions[0] == nil {
		return
	}

	for _, a := range d.transactions[d.getCurrentTransactionID()].actions {
		switch a.aType {
		case ActionSet:
			d.storage[a.key] = a.value
		case ActionDelete:
			delete(d.storage, a.key)
		}
	}

	delete(d.transactions, d.getCurrentTransactionID())
}

func (d *InMemoryDB) Rollback() {
	// no active transactions, skip
	if d.transactions[0] == nil {
		return
	}

	delete(d.transactions, d.getCurrentTransactionID())
}

func (d *InMemoryDB) getCurrentTransactionID() int {
	if len(d.transactions) == 0 {
		return 0
	}

	return len(d.transactions) - 1
}
