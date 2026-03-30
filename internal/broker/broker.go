package broker

import (
	"github.com/HunterXIII/MyBroker/internal/models"
)

type Broker struct {
	Topics map[string]*models.Topic
}
