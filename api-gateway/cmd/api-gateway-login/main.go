// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"

	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	ServerURL    string `envconfig:"HEIMDALL_SERVER_URL" default:"http://localhost:8080"`
	LoginURL     string `envconfig:"HEIMDALL_LOGIN_URL" default:"/v1/auth/login"`
	CallbackURL  string `envconfig:"HEIMDALL_CALLBACK_URL" default:"/v1/auth/callback"`
	LogoutURL    string `envconfig:"HEIMDALL_LOGOUT_URL" default:"/v1/auth/logout"`
	RefreshURL   string `envconfig:"HEIMDALL_REFRESH_URL" default:"/v1/auth/refresh"`
	AccessToken  string `envconfig:"HEIMDALL_ACCESS_TOKEN" default:""`
	IdToken      string `envconfig:"HEIMDALL_ID_TOKEN" default:""`
	RefreshToken string `envconfig:"HEIMDALL_REFRESH_TOKEN" default:""`
}

func main() {
	var config Config
	if err := envconfig.Process("", &config); err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	var lastCookies []*http.Cookie

	// Set cookies if they are provided
	if config.AccessToken != "" {
		cookie := &http.Cookie{Name: "accessToken", Value: config.AccessToken}
		lastCookies = append(lastCookies, cookie)
	}
	if config.IdToken != "" {
		cookie := &http.Cookie{Name: "idToken", Value: config.IdToken}
		lastCookies = append(lastCookies, cookie)
	}
	if config.RefreshToken != "" {
		cookie := &http.Cookie{Name: "refreshToken", Value: config.RefreshToken}
		lastCookies = append(lastCookies, cookie)
	}

	targetURL, err := url.Parse(config.ServerURL)
	if err != nil {
		log.Fatalf("Failed to parse internal server URL: %v", err)
	}

	internalServerURL, err := url.Parse(config.ServerURL)
	if err != nil {
		log.Fatalf("Failed to parse internal server URL: %v", err)
	}

	proxyUrl := *internalServerURL
	proxyUrl.Path = ""
	proxyUrl.RawQuery = ""

	fmt.Printf("proxy url: %s\n", proxyUrl.String())

	proxy := httputil.NewSingleHostReverseProxy(&proxyUrl)

	// start callback mux server
	mux := http.NewServeMux()
	mux.HandleFunc("/login", func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("callback")

		// create login url with redirect uri and session id
		loginURL := fmt.Sprintf("%s?redirectURI=%s", config.ServerURL+config.LoginURL, "http://localhost:8082/status")

		fmt.Printf("redirect url: %s\n", loginURL)

		// redirect to the login url
		http.Redirect(w, r, loginURL, http.StatusFound)
	})

	mux.HandleFunc("/logout", func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("logout")
		http.Redirect(w, r, config.ServerURL+config.LogoutURL, http.StatusFound)
	})

	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("status")

		fmt.Printf("refresh url: %s\n", config.ServerURL+config.RefreshURL)

		// Request /v1/auth/token with cookies set
		client := &http.Client{}
		req, err := http.NewRequest("GET", config.ServerURL+config.RefreshURL, nil)
		if err != nil {
			fmt.Printf("failed to create request: %v\n", err)
		}

		// add all cookies from r
		for _, cookie := range r.Cookies() {
			fmt.Printf("adding cookie: %v\n", cookie.Name)
			req.AddCookie(cookie)
		}
		resp, err := client.Do(req)
		if err != nil {
			fmt.Printf("failed to request token: %v\n", err)
			return
		}

		if resp.StatusCode != http.StatusOK {
			fmt.Printf("failed to request token: %v\n", resp.StatusCode)
			http.Error(w, "failed to request token", http.StatusUnauthorized)
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				fmt.Printf("failed to read response body: %v\n", err)
			}
			fmt.Printf("response body: %s\n", string(body))

			return
		}

		defer func() {
			if err := resp.Body.Close(); err != nil {
				fmt.Printf("failed to close response body: %v\n", err)
			}
		}()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			fmt.Printf("failed to read response body: %v\n", err)
		}
		fmt.Println(string(body))

		lastCookies = resp.Cookies()

		// Write all cookies to the response
		for _, cookie := range resp.Cookies() {
			fmt.Printf("adding cookie to response: %v\n", cookie.Name)
			http.SetCookie(w, cookie)
		}

		_, err = w.Write([]byte("status: logged in\n"))
		if err != nil {
			fmt.Printf("failed to write response: %v\n", err)
		}

	})

	// Default handler for all other paths
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {

		// Proxy the request to the internal server
		for _, cookie := range lastCookies {
			r.AddCookie(cookie)
		}

		r.URL.Host = targetURL.Host
		r.URL.Scheme = targetURL.Scheme
		r.URL.Path = targetURL.Path + r.URL.Path

		r.Header.Set("Host", targetURL.Host)

		fmt.Printf("proxying request to %s\n", r.URL.String())

		proxy.ServeHTTP(w, r)
	})

	err = http.ListenAndServe(":8082", mux)
	if err != nil {
		fmt.Printf("failed to start server: %v\n", err)
		os.Exit(1)
	}
}
