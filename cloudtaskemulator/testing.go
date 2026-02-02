package cloudtaskemulator

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/nolanrsherman/gcpemulators/cloudtaskemulator/db"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestingNewCloudTaskEmulator(t *testing.T, mongoURI string) (*CloudTaskEmulator, func(t *testing.T)) {
	t.Helper()

	// find a free port
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	listener.Close()
	port := listener.Addr().(*net.TCPAddr).Port

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	mongoDB, cleanupDB := db.NewTestDatabase(context.Background(), t, mongoURI)
	emulator := NewCloudTaskEmulator(mongoURI, mongoDB.Name(), logger, port)

	emuCtx, emuCtxCancel := context.WithCancel(context.Background())
	runError := make(chan error, 1)
	go func() {
		runError <- emulator.Run(emuCtx)
	}()
	select {
	case err := <-runError: //we will need to check this again later
		panic(err)
	case <-time.After(100 * time.Millisecond):
		return emulator, func(t *testing.T) {
			emuCtxCancel()
			select {
			case err := <-runError:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(10 * time.Second):
				require.FailNow(t, "cloud task emulator did not stop")
			}
			cleanupDB(t)
		}
	}
}
