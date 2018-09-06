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
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/mhelmich/tf-locker/backend"
	"github.com/sirupsen/logrus"
)

func main() {
	logrus.Infof("Starting tf-locker...")
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)

	strPort := os.Getenv("PORT")
	if strPort == "" {
		strPort = "8080"
	}
	httpPort, err := strconv.Atoi(strPort)
	if err != nil {
		logrus.Panicf("Can't parse port [%s]: %s", strPort, err.Error())
	}

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable", "franz", "passwd", "franz")
	}

	logrus.Infof("Connecting to postgres at %s", dbURL)
	db, err := backend.NewPostgresStore(dbURL)
	if err != nil {
		logrus.Panicf("Can't parse port [%s]: %s", strPort, err.Error())
	}

	logrus.Infof("Start REST service at %d", httpPort)
	httpServer, err := startNewHTTPServer(httpPort, db)
	if err != nil {
		logrus.Panicf("Can't start http server: %s", err.Error())
	}

	sig := <-c
	cleanup(sig, httpServer, db)
}

func cleanup(sig os.Signal, httpServer *httpServer, store backend.Store) {
	logrus.Info("This node is going down gracefully\n")
	logrus.Infof("Received signal: %s\n", sig)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	httpServer.Shutdown(ctx)

	store.Close()

	logrus.Exit(0)
}
