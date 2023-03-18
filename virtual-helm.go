package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

var blobs = make(map[string][]byte)

type Config struct {
	MediaType string `json:"mediaType"`
	Digest    string `json:"digest"`
	Size      int    `json:"size"`
}

type Layer struct {
	MediaType string `json:"mediaType"`
	Digest    string `json:"digest"`
	Size      int    `json:"size"`
}

type Manifest struct {
	SchemaVersion int     `json:"schemaVersion"`
	Config        Config  `json:"config"`
	Layers        []Layer `json:"layers"`
}

type Chart struct {
	ApiVersion  string `json:"apiVersion"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Type        string `json:"type"`
	Version     string `json:"version"`
	AppVersion  string `json:"appVersion"`
}

func getChart(name string, reference string) ([]byte, error) {
	chart := Chart{
		ApiVersion:  "v2",
		Name:        name,
		Description: "A dynamically generated chart",
		Type:        "application",
		Version:     "0.1.0",
		AppVersion:  time.Now().Format(time.RFC822),
	}

	return json.Marshal(chart)
}

func getChartContent(name string, reference string) ([]byte, error) {
	content := []byte("Hello helm!")
	tarballBuf := new(bytes.Buffer)
	tarball := tar.NewWriter(tarballBuf)

	header := &tar.Header{
		Typeflag: tar.TypeReg,
		Name:     "README.md",
		Size:     int64(len(content)),
		Mode:     0644,
	}
	err := tarball.WriteHeader(header)
	if err != nil {
		return nil, err
	}

	c := bytes.NewReader(content)
	fmt.Println(c.Size())
	_, err = io.Copy(tarball, c)
	if err != nil {
		return nil, err
	}

	fmt.Println("Tar size: ", tarballBuf.Len(), " bytes")
	fmt.Println(string(tarballBuf.Bytes()))

	tarball.Flush()
	tarball.Close() // Must write footer before returning the buffer

	gzBuffer := new(bytes.Buffer)
	gz := gzip.NewWriter(gzBuffer)

	io.Copy(gz, tarballBuf)

	gz.Close()
	return gzBuffer.Bytes(), nil
}

func writeManifest(w http.ResponseWriter, name string, reference string) error {
	fmt.Println("Manifest")

	chart, err := getChart(name, reference)
	if err != nil {
		return err
	}

	chartTar, err := getChartContent(name, reference)
	if err != nil {
		return err
	}

	h := sha256.New()
	h.Write(chart)
	digest := fmt.Sprintf("sha256:%x", h.Sum(nil))
	blobs[digest] = chart

	h.Reset()
	h.Write(chartTar)

	chartContentDigest := fmt.Sprintf("sha256:%x", h.Sum(nil))
	blobs[chartContentDigest] = chartTar

	manifest := Manifest{
		SchemaVersion: 2,
		Config: Config{
			MediaType: "application/vnd.cncf.helm.config.v1+json",
			Digest:    digest,
			Size:      len(chart),
		},
		Layers: []Layer{{
			MediaType: "application/vnd.cncf.helm.chart.content.v1.tar+gzip",
			Digest:    chartContentDigest,
			Size:      len(chartTar),
		}},
	}

	w.Header().Add("content-type", "application/vnd.oci.image.manifest.v1+json")
	w.Header().Add("Docker-Content-Digest", manifest.Config.Digest)
	w.WriteHeader(http.StatusOK)

	e := json.NewEncoder(w)
	e.Encode(manifest)

	return nil
}

func writeBlob(w http.ResponseWriter, name string, digest string) error {
	blob, ok := blobs[digest]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return nil
	}

	fmt.Printf("blob size: %d\n", len(blob))
	w.Write(blob)
	return nil
}

func handleV2(w http.ResponseWriter, r *http.Request) {
	fmt.Printf("%s %s\n", r.Method, r.URL)
	if r.Method == "POST" {
		w.Header().Add("Location", "http://localhost:5000/v2/blobs/put/"+uuid.NewString())
		w.WriteHeader(http.StatusAccepted)
		return
	}

	if r.Method == "PUT" {
		digest := r.URL.Query().Get("digest")
		w.Header().Add("location", "https://localhost:5000/v2/blobs/"+digest)
		w.Header().Add("Docker-Content-Digest", digest)
		w.WriteHeader(http.StatusCreated)
		body, _ := ioutil.ReadAll(r.Body)
		fmt.Printf("\n\n%s\n\n", body)
		return
	}

	if r.Method == "HEAD" {
		w.WriteHeader(http.StatusOK)
		return
	}

	tokens := strings.Split(r.URL.Path, "/")
	if len(tokens) < 3 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	refOrDigest := tokens[len(tokens)-1]
	objType := tokens[len(tokens)-2]
	name := strings.Join(tokens[2:len(tokens)-2], "/")

	var err error
	switch objType {
	case "manifests":
		fmt.Printf("Accept header: %s\n", r.Header.Get("Accept"))
		err = writeManifest(w, name, refOrDigest)
	case "blobs":
		err = writeBlob(w, name, refOrDigest)
	default:
		err = fmt.Errorf("unknown request type: %s", objType)
	}

	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
	}

}

func main() {
	http.HandleFunc("/v2/", handleV2)

	fmt.Println("Starting server")
	err := http.ListenAndServe(":5000", nil)
	if err != nil {
		panic(err)
	}

}
