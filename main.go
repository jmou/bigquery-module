// XXX handle in/tables resource dependencies; templatize the query?
// or use CTE (with query munging if user query has CTE):
// WITH $table AS (SELECT * FROM $backing)
// XXX use materialized views?
// XXX story for credentials

package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/google/uuid"
	"google.golang.org/api/iterator"
)

// XXX naming ambiguity
type Resource struct {
	projectID string
	datasetID string
	tableID   string
	tamper    string
}

// XXX https://pkg.go.dev/time#Time.MarshalText
type ResourceMetadata struct {
	FullID           string
	CreationTime     time.Time
	LastModifiedTime time.Time
	ETag             string
}

func slurp(path string) (string, error) {
	contents, err := ioutil.ReadFile(path)
	if err != nil {
		return "", err
	}
	trimmed := bytes.Trim(contents, "\n")
	return string(trimmed), nil
}

func copyFile(src string, dest string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	destFile, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, srcFile)
	return err
}

func setupSession() (string, error) {
	session, err := slurp("in/session")
	if err != nil {
		return "", err
	}

	var projectID string
	for _, line := range strings.Split(session, "\n") {
		pieces := strings.SplitN(line, "=", 2)
		switch pieces[0] {
		case "credentials":
			// XXX hacky global environment variable
			os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", pieces[1])
		case "projectid":
			projectID = pieces[1]
		}
	}
	return projectID, nil
}

func tableResource(table *bigquery.Table) (*ResourceMetadata, error) {
	// XXX manage Context
	ctx := context.Background()

	md, err := table.Metadata(ctx)
	if err != nil {
		return nil, err
	}
	// XXX schema
	return &ResourceMetadata{
		FullID:           md.FullID,
		CreationTime:     md.CreationTime,
		LastModifiedTime: md.LastModifiedTime,
		ETag:             md.ETag,
	}, nil
}

// XXX revamp parsing
// XXX ensure all fields are populated
func parseResource(path string) (*Resource, error) {
	contents, err := slurp(path)
	if err != nil {
		return nil, err
	}

	var resource Resource
	for _, line := range strings.Split(contents, "\n") {
		pieces := strings.SplitN(line, "=", 2)
		if len(pieces) != 2 {
			return nil, fmt.Errorf("malformed resource")
		}
		key, value := pieces[0], pieces[1]
		switch key {
		case "store":
			if value != "bigquery" {
				return nil, fmt.Errorf("not a bigquery resource")
			}
		case "projectid":
			resource.projectID = value
		case "datasetid":
			resource.datasetID = value
		case "tableid":
			resource.tableID = value
		case "tamper":
			resource.tamper = value
		}
	}
	return &resource, nil
}

func queryToTable(projectID string, datasetID string, tableID string, query string) (*bigquery.Table, error) {
	_, err := setupSession()
	if err != nil {
		return nil, err
	}

	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	dataset := client.Dataset(datasetID)
	// XXX DatasetMetadata with description, location
	if err := dataset.Create(ctx, nil); err != nil {
		// XXX properly handle duplicate?
		// return nil, err
	}

	table := client.Dataset(datasetID).Table(tableID)
	q := client.Query(query)
	q.QueryConfig.Dst = table
	job, err := q.Run(ctx)
	if err != nil {
		return nil, err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return nil, err
	}
	if err := status.Err(); err != nil {
		return nil, err
	}

	return table, nil
}

func transform() error {
	projectID, err := setupSession()
	if err != nil {
		return err
	}

	// XXX make dataset a module dependency
	datasetID := "knit"
	tableID := uuid.New().String()
	query, err := slurp("in/query")
	if err != nil {
		return err
	}

	dir := "in/tables/"
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	// XXX string builder?
	var cte string
	for _, entry := range entries {
		if len(cte) == 0 {
			cte += "WITH "
		} else {
			cte += ",\n"
		}
		cte += entry.Name()
		cte += " AS (SELECT * FROM "
		resource, err := parseResource(filepath.Join(dir, entry.Name()))
		if err != nil {
			return err
		}
		// XXX ` quoting?
		cte += resource.projectID
		cte += "."
		cte += resource.datasetID
		cte += "."
		cte += resource.tableID
		cte += ")"
	}
	cte += "\n\n"

	// XXX output the full query
	table, err := queryToTable(projectID, datasetID, tableID, cte+query)
	if err != nil {
		return err
	}

	resource, err := tableResource(table)
	if err != nil {
		return err
	}

	file, err := os.Create("out/table")
	if err != nil {
		return err
	}
	defer file.Close()

	// XXX tamper should be more canonical, like UTC
	_, err = fmt.Fprintf(file,
		"datasetid=%s\nprojectid=%s\nstore=bigquery\ntableid=%s\ntamper=%+v\n",
		datasetID, projectID, tableID, *resource)
	return err
}

func gate() error {
	ctx := context.Background()

	tr, err := parseResource("in/table")
	if err != nil {
		return err
	}

	_, err = setupSession()
	if err != nil {
		return err
	}

	client, err := bigquery.NewClient(ctx, tr.projectID)
	if err != nil {
		return err
	}
	defer client.Close()

	table := client.Dataset(tr.datasetID).Table(tr.tableID)
	resource, err := tableResource(table)
	if err != nil {
		return err
	}

	actual := fmt.Sprintf("%+v", *resource)
	if actual != tr.tamper {
		return fmt.Errorf("tamper detected\n%s\n%s", tr.tamper, actual)
	}

	err = copyFile("in/table", "out/table")
	return err
}

// XXX DRY with transform()
// XXX lift gate should refresh not die
func lift() error {
	ctx := context.Background()

	projectID, err := slurp("in/projectid")
	if err != nil {
		return err
	}
	datasetID, err := slurp("in/datasetid")
	if err != nil {
		return err
	}
	tableID, err := slurp("in/tableid")
	if err != nil {
		return err
	}

	_, err = setupSession()
	if err != nil {
		return err
	}

	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return err
	}
	defer client.Close()

	table := client.Dataset(datasetID).Table(tableID)
	resource, err := tableResource(table)
	if err != nil {
		return err
	}

	file, err := os.Create("out/table")
	if err != nil {
		return err
	}
	defer file.Close()

	// XXX tamper should be more canonical, like UTC
	_, err = fmt.Fprintf(file,
		"datasetid=%s\nprojectid=%s\nstore=bigquery\ntableid=%s\ntamper=%+v\n",
		datasetID, projectID, tableID, *resource)
	return err
}

func preview(credentialsFile string, projectID string, source string) error {
	// DRY with setupSession
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", credentialsFile)

	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return err
	}
	defer client.Close()

	q := client.Query(fmt.Sprintf("SELECT * FROM `%s` LIMIT 100", source))
	it, err := q.Read(ctx)
	if err != nil {
		return err
	}
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		// XXX
		fmt.Println(row)
	}
	return nil
}

func main() {
	// XXX hack for tamper time zone
	os.Setenv("TZ", "America/New_York")

	var err error
	switch os.Args[1] {
	case "transform":
		err = transform()
	case "gate":
		err = gate()
	case "lift":
		err = lift()
	case "preview":
		err = preview(os.Args[2], os.Args[3], os.Args[4])
	}
	if err != nil {
		log.Fatalf("error: %v", err)
	}
}
