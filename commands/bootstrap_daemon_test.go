package commands_test

import (
	"testing"

	th "github.com/filecoin-project/go-filecoin/testhelpers"
	tf "github.com/filecoin-project/go-filecoin/testhelpers/testflags"

	"github.com/stretchr/testify/assert"
)

func TestBootstrapList(t *testing.T) {
	tf.IntegrationTest(t)

	assert := assert.New(t)

	d := th.NewDaemon(t).Start()
	defer d.ShutdownSuccess()

	bs := d.RunSuccess("bootstrap ls")

	assert.Equal("&{[]}\n", bs.ReadStdout())
}
