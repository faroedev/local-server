package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/faroedev/faroe"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "Missing port argument")
		return
	}
	portValue := os.Args[1]
	port, err := strconv.Atoi(portValue)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Invalid port argument")
		return
	}

	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "Missing user_server_action_invocation_endpoint argument")
		return
	}
	userServerActionInvocationEndpoint := os.Args[2]
	userActionInvocationEndpointClient := newPublicActionInvocationEndpointClient(userServerActionInvocationEndpoint)

	storage := newStorage()
	userServerClient := faroe.NewUserServerClient(userActionInvocationEndpointClient)
	logger := newStderrActionsLogger()
	userPasswordHashAlgorithm := newArgon2id(3, 1024*64, 1)
	temporaryPasswordHashAlgorithm := newArgon2id(3, 1024*16, 1)
	emailSender := newStdoutActionsEmailSender()

	server := faroe.NewServer(
		storage,
		userServerClient,
		logger,
		[]faroe.PasswordHashAlgorithmInterface{userPasswordHashAlgorithm},
		temporaryPasswordHashAlgorithm,
		runtime.NumCPU(),
		faroe.RealClock,
		faroe.AllowAllEmailAddresses,
		emailSender,
		faroe.SessionConfigStruct{
			InactivityTimeout:     30 * 24 * time.Hour,
			ActivityCheckInterval: time.Minute,
			UserCacheExpiration:   time.Minute,
		},
	)

	fmt.Printf("Starting server at port %d...\n", port)
	err = http.ListenAndServe(fmt.Sprintf(":%d", port), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "OPTIONS" && r.URL.Path == "/" {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			w.WriteHeader(http.StatusNoContent)
			return
		}

		if r.Method == "POST" && r.URL.Path == "/" {
			bodyBytes, err := io.ReadAll(r.Body)
			if err != nil {
				w.Header().Set("Access-Control-Allow-Origin", "*")
				w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			resultJSON, err := server.ResolveActionInvocationEndpointRequestWithBlocklist(string(bodyBytes), nil)
			if err != nil {
				w.Header().Set("Access-Control-Allow-Origin", "*")
				w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(resultJSON))
			return
		}

		w.WriteHeader(http.StatusNotFound)
	}))
	if err != nil {
		panic(err)
	}
}

type publicActionInvocationEndpointClientStruct struct {
	endpoint string
}

func newPublicActionInvocationEndpointClient(endpoint string) *publicActionInvocationEndpointClientStruct {
	return &publicActionInvocationEndpointClientStruct{endpoint}
}

func (publicActionInvocationEndpointClient *publicActionInvocationEndpointClientStruct) SendActionInvocationEndpointRequest(body string) (string, error) {
	request, _ := http.NewRequest("POST", publicActionInvocationEndpointClient.endpoint, strings.NewReader(body))

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return "", fmt.Errorf("failed to send request: %s", err.Error())
	}
	if response.StatusCode != http.StatusOK {
		response.Body.Close()
		return "", fmt.Errorf("unexpected status code %d", response.StatusCode)
	}
	bodyBytes, err := io.ReadAll(response.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response body: %s", err.Error())
	}
	return string(bodyBytes), nil
}
