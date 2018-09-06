/*
 * Copyright 2018 Marco Helmich
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/mhelmich/tf-locker/backend"
	"github.com/sirupsen/logrus"
)

type httpServer struct {
	http.Server

	store backend.Store
}

func startNewHTTPServer(port int, store backend.Store) (*httpServer, error) {
	router := mux.NewRouter().StrictSlash(true)
	httpServer := &httpServer{
		Server: http.Server{
			Addr:         fmt.Sprintf(":%d", port),
			Handler:      router,
			WriteTimeout: time.Second * 60,
			ReadTimeout:  time.Second * 60,
			IdleTimeout:  time.Second * 60,
		},
		store: store,
	}

	router.
		Methods("GET").
		Path("/state/{name}/{state_id}").
		HandlerFunc(httpServer.getState).
		Name("getState")

	router.
		Methods("POST", "PUT").
		Path("/state/{name}/{state_id}").
		HandlerFunc(httpServer.setState).
		Name("setState")

	router.
		Methods("DELETE").
		Path("/state/{name}/{state_id}").
		HandlerFunc(httpServer.deleteState).
		Name("deleteState")

	router.
		Methods("LOCK").
		Path("/state/{name}/{state_id}").
		HandlerFunc(httpServer.lockState).
		Name("lockState")

	router.
		Methods("UNLOCK").
		Path("/state/{name}/{state_id}").
		HandlerFunc(httpServer.unlockState).
		Name("unlockState")

	go httpServer.ListenAndServe()
	return httpServer, nil
}

func (s *httpServer) getState(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]
	stateID := vars["state_id"]
	defer r.Body.Close()

	data, err := s.store.GetState(stateID, name)
	if err != nil {
		logrus.Errorf("Get didn't work: %s", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	var b64 string
	if len(data) > 0 {
		b64 = md5Hash(data)
		logrus.Infof("send data: %d %s", len(data), b64)
		w.Header().Set("Content-MD5", b64)
		w.Write(data)
	}

	logrus.Infof("GET: %s %s %d %s", name, stateID, len(data), b64)
}

func (s *httpServer) setState(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]
	stateID := vars["state_id"]
	defer r.Body.Close()

	err := s.validateIDs(name, stateID)
	if err != nil {
		logrus.Errorf("Invalid state_id: %s %s", name, stateID)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		logrus.Errorf("Can't deserialize request body: %s", err.Error())
	}
	defer r.Body.Close()

	// lockID := vars["ID"]
	lockID := r.URL.Query().Get("ID")
	if lockID == "" {
		logrus.Info("Empty lock id...")
	}

	err = s.store.UpsertState(stateID, name, lockID, body)
	if err != nil {
		logrus.Errorf("Can't upsert state: %s", err.Error())
	}

	w.WriteHeader(http.StatusOK)
	logrus.Infof("SET: %s %s %d %s", name, stateID, len(body), md5Hash(body))
}

func (s *httpServer) deleteState(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]
	stateID := vars["state_id"]
	logrus.Infof("Deleting state: %s %s", name, stateID)
	defer r.Body.Close()

	err := s.store.DeleteState(stateID, name)
	if err != nil {
		logrus.Errorf("Can't delete state [%s] [%s]: %s", name, stateID, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	logrus.Infof("DELETE: %s %s", name, stateID)
}

func (s *httpServer) lockState(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]
	stateID := vars["state_id"]

	// query database to see whether a lock state exists already
	// if not, return 200
	// if it does, return error and put the lock info into the body
	// http.StatusConflict, http.StatusLocked:
	// https://www.terraform.io/docs/backends/types/http.html

	defer r.Body.Close()
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		logrus.Errorf("Can't read request body: %s", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// logrus.Infof("LOCK: lock info: %s", string(body))

	// li := &state.LockInfo{}
	// err = json.Unmarshal(body, li)
	// if err != nil {
	// 	logrus.Errorf("Can't deserialize request body: %s", err.Error())
	// 	w.WriteHeader(http.StatusInternalServerError)
	// 	return
	// }

	err = s.store.LockState(stateID, name, string(body))
	if err == backend.ErrAlreadyLocked {
		logrus.Infof("LOCK: already locked %s %s", name, stateID)
		w.WriteHeader(http.StatusLocked)
		return
	} else if err != nil {
		logrus.Errorf("locking failed [%s] [%s]: %s", name, stateID, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	logrus.Infof("LOCK: %s %s", name, stateID)
}

func (s *httpServer) unlockState(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]
	stateID := vars["state_id"]
	defer r.Body.Close()

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		logrus.Errorf("Can't deserialize request body: %s", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// logrus.Infof("UNLOCK: lock info: %s", string(body))

	err = s.store.UnlockState(stateID, name, string(body))
	if err != nil {
		logrus.Errorf("unlocking failed [%s] [%s]: %s", name, stateID, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	logrus.Infof("UNLOCK: %s %s", name, stateID)
}

func (s *httpServer) validateIDs(name string, id string) error {
	_, err := uuid.Parse(id)
	if err != nil {
		return fmt.Errorf("Can't parse uuid [%s]: %s", id, err.Error())
	}

	if len(name) > 64 {
		return fmt.Errorf("String too long (> 64): %s", name)
	}

	return nil
}

func md5Hash(data []byte) string {
	hash := md5.Sum(data)
	return base64.StdEncoding.EncodeToString(hash[:])
}
