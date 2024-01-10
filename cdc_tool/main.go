package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"text/template"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/spf13/cobra"
	"github.com/tidwall/pretty"
)

func (s *CDCService) applyChange(ctx context.Context, record *neo4j.Record) error {
	m := asMap(record)
	event := m["event"].(map[string]any)
	jsonOutput, err := json.Marshal(asMap(record))
	changeId, ok := m["id"].(string)
	if !ok {
		return fmt.Errorf("No change identifier in change event.")
	}
	if err != nil {
		return fmt.Errorf("unable to jsonify record: %w", err)
	}

	fmt.Println(string(pretty.Color(pretty.Pretty(jsonOutput), pretty.TerminalStyle)))
	var (
		cypher string
		params map[string]any
	)
	switch event["operation"].(string) {
	case "c":
		cypher, params, err = buildCreateQuery(event)
		if err != nil {
			panic(err)
		}

	case "u":
		cypher, params, err = buildUpdateQuery(event)
		if err != nil {
			panic(err)
		}
	case "d":
		cypher, params, err = buildDeleteQuery(event)
		if err != nil {
			panic(err)
		}
	}

	if cypher != "" {
		session := s.sink.NewSession(ctx, neo4j.SessionConfig{DatabaseName: s.database})
		_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
			_, err := tx.Run(ctx, cypher, params)
			if err != nil {
				return nil, err
			}

			_, err = storeChangeIdentifier(ctx, tx, changeId)
			if err != nil {
				return nil, err
			}
			return nil, nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func queryChangeID(ctx context.Context, driver neo4j.DriverWithContext, database string, query string) (string, error) {
	result, err := neo4j.ExecuteQuery(ctx, driver, query, nil, neo4j.EagerResultTransformer, neo4j.ExecuteQueryWithDatabase(database), neo4j.ExecuteQueryWithReadersRouting())
	if err != nil {
		return "", fmt.Errorf("unable to query change identifier: %w", err)
	}

	if len(result.Records) != 1 {
		return "", fmt.Errorf("expected one record, but got %d", len(result.Records))
	}

	id, _, err := neo4j.GetRecordValue[string](result.Records[0], "id")
	if err != nil {
		return "", fmt.Errorf("unable to extract id: %w", err)
	}

	return id, nil
}

func asMap(record *neo4j.Record) map[string]any {
	result := make(map[string]any, len(record.Keys))

	for i := 0; i < len(record.Keys); i++ {
		result[record.Keys[i]] = record.Values[i]
	}

	return result
}

const storeChangeIdentifierCypher = `
MERGE (c:ChangeIdentifier)
SET c.value = $identifier
`

func storeChangeIdentifier(ctx context.Context, tx neo4j.ManagedTransaction, identifier string) (neo4j.ManagedTransaction, error) {
	_, err := tx.Run(ctx, storeChangeIdentifierCypher, map[string]any{"identifier": identifier})
	if err != nil {
		return nil, err
	}
	return tx, nil
}

// nodePattern returns a cypher string for a single node
// along with the parameters necessary to create it given the output of a CDC record.
//
// Parameters:
//
//	{
//	   "event": {
//		     "keys": {
//	         Some Label: {
//	            some attribute key: attribute value,
//				...
//	         },
//	      }
//	   }
//	}
//
// Returns:
// ("(n:Unique:Foo {test: $value_1})", {"value_1": 1})
func nodePattern(event map[string]any, nodeVar string) (string, map[string]any) {
	// Gather up every label and every set of attributes
	attributes := make(map[string]any, 0)
	labelString := ""
	for label, attribs := range event["keys"].(map[string]any) {
		maps.Copy(attributes, attribs.(map[string]any))
		labelString += ":" + label
	}
	// We need two things to add attributes to our node
	//    1. We need a string with the attributes we will merge on parametrized, e.g.
	//       "{type: $value_1, external_id: $value_2, id: $value_3}"
	//	  2. We need a map of the actual parameters, e.g.
	//		 {$value_1: "Car", external_id: "123Foo", id: "456Bar"}
	attributePairs := make([]string, 0)
	paramTemplate := fmt.Sprintf("%s_merge_value_", nodeVar)
	parameters := make(map[string]any, 0)
	var valueString string
	cnt := 0
	for k, v := range attributes {
		valueString = paramTemplate + strconv.Itoa(cnt)
		attributePairs = append(attributePairs, fmt.Sprintf("%s: $%s", k, valueString))
		parameters[valueString] = v
		cnt++
	}
	attributeString := ""
	if len(attributePairs) > 0 {
		attributeString = "{" + strings.Join(attributePairs, ", ") + "}"
	}
	nodePattern := "(" + nodeVar + labelString + " " + attributeString + ")"
	return nodePattern, parameters
}

// labelPatterns returns the string for setting node labels,
//
// Returns
// "varName:Foo:Bar"
func labelPattern(state map[string]any, varName string) string {
	labelString := ""
	for _, label := range state["labels"].([]any) {
		labelString += ":" + label.(string)
	}
	return labelString
}

// relPattern
func relPattern(event map[string]any, startVar string, varName string, endVar string) (string, map[string]any) {
	var (
		attrVar string
	)
	params := make(map[string]any, 0)
	attrs := ""
	attrTemplate := fmt.Sprintf("%s_rel_attr_", varName)
	cnt := 0
	for k, v := range event["key"].(map[string]any) {
		attrVar = attrTemplate + strconv.Itoa(cnt)
		attrs += fmt.Sprintf("%s: %s", k, attrVar)
		params[attrVar] = v
		cnt++
	}
	if attrs != "" {
		attrs = fmt.Sprintf("{%s}", attrs)
	}
	rel := fmt.Sprintf("(%s)-[%s:%s %s]->(%s)", startVar, varName, event["type"].(string), attrs, endVar)
	return rel, params
}

// attribPattern
func attribPattern(state map[string]any, varName string) (string, map[string]any) {
	after := state["after"].(map[string]any)
	existingProperties := make(map[string]any, 0)
	if v, ok := state["before"].(map[string]any); ok && v != nil {
		existingProperties = v["properties"].(map[string]any)
	}
	attrStrings := make([]string, 0)
	attrParams := make(map[string]any, 0)
	paramTemplate := fmt.Sprintf("%s_value_", varName)
	cnt := 0
	for k, newVal := range after["properties"].(map[string]any) {
		if oldVal, ok := existingProperties[k]; ok && oldVal == newVal {
			continue
		}
		paramVar := paramTemplate + strconv.Itoa(cnt)
		attrStrings = append(attrStrings, fmt.Sprintf("%s.%s=$%s", varName, k, paramVar))
		attrParams[paramVar] = newVal
		cnt++
	}
	return strings.Join(attrStrings, ", "), attrParams
}

func buildDeleteQuery(event map[string]any) (string, map[string]any, error) {
	var (
		tmp bytes.Buffer
	)
	switch event["eventType"] {
	case "r":
		startCypher, startParams := nodePattern(event["start"].(map[string]any), "start")
		endCypher, endParams := nodePattern(event["end"].(map[string]any), "end")
		relCypher, relKeyParams := relPattern(event, "start", "rel", "end")
		maps.Copy(startParams, endParams)
		maps.Copy(startParams, relKeyParams)
		t := template.Must(template.New("deleteRelCypher").Parse(deleteRelCypher))
		if err := t.Execute(&tmp, map[string]string{
			"start":  startCypher,
			"end":    endCypher,
			"rel":    relCypher,
			"relVar": "rel",
		}); err != nil {
			return "", nil, err
		}
		return tmp.String(), startParams, nil
	case "n":
		matchCypher, matchParams := nodePattern(event, "n")
		t := template.Must(template.New("deleteNodeCypher").Parse(deleteNodeCypher))
		if err := t.Execute(&tmp, map[string]string{
			"pattern": matchCypher,
			"varName": "n",
		}); err != nil {
			return "", nil, err
		}
		return tmp.String(), matchParams, nil
	}
	return "", nil, nil
}

const deleteNodeCypher = `MATCH {{.pattern}}
DETACH DELETE {{.varName}}
`

const deleteRelCypher = `MATCH {{.start}}
MATCH {{.end}}
MATCH {{.rel}}
DELETE {{.relVar}}
`

func buildUpdateQuery(event map[string]any) (string, map[string]any, error) {
	var (
		tmp bytes.Buffer
	)
	switch event["eventType"] {
	case "r":
		startCypher, startParams := nodePattern(event["start"].(map[string]any), "start")
		endCypher, endParams := nodePattern(event["end"].(map[string]any), "end")
		relCypher, relKeyParams := relPattern(event, "start", "rel", "end")
		state := event["state"].(map[string]any)
		attrs, relParams := attribPattern(state, "rel")
		maps.Copy(startParams, endParams)
		maps.Copy(startParams, relKeyParams)
		maps.Copy(startParams, relParams)
		t := template.Must(template.New("updateRelCypher").Parse(updateRelCypher))
		if err := t.Execute(&tmp, map[string]string{
			"start":             startCypher,
			"end":               endCypher,
			"rel":               relCypher,
			"assignmentPattern": attrs,
		}); err != nil {
			return "", nil, err
		}
		return tmp.String(), startParams, nil
	case "n":
		state := event["state"].(map[string]any)
		after := state["after"].(map[string]any)
		matchCypher, matchParams := nodePattern(event, "n")
		labelString := labelPattern(after, "n")
		attribString, attribParams := attribPattern(state, "n")
		maps.Copy(matchParams, attribParams)
		t := template.Must(template.New("updateNodeCypher").Parse(updateNodeCypher))
		if err := t.Execute(&tmp, map[string]string{
			"matchPattern":      matchCypher,
			"assignmentPattern": attribString,
			"labelPattern":      labelString,
		}); err != nil {
			return "", nil, err
		}
		return tmp.String(), matchParams, nil
	}
	return "", nil, nil
}

const updateRelCypher = `MATCH {{.start}}
MATCH {{.end}}
MATCH {{.rel}}
SET {{.assignmentPattern}}`

const updateNodeCypher = `MATCH {{.matchPattern}}
SET {{.attributePattern}}
{{with .labelPattern}}SET {{.labelPattern}}{{end}}`

// buildCreateQuery
//
// Returns:
// A create cypher, e.g.
// `CREATE (n:Node {id: merge_value_0})
// SET n:Node:DigitialTwin
// SET n.var=$update_value_0, n.type=$update_value_1`
//
// and a set of parameters `{"merge_value_0"=123, "update_value_0": foo, ...}`
func buildCreateQuery(event map[string]any) (string, map[string]any, error) {
	var (
		tmp bytes.Buffer
	)
	if event["eventType"] == "r" {
		startCypher, startParams := nodePattern(event["start"].(map[string]any), "start")
		endCypher, endParams := nodePattern(event["end"].(map[string]any), "end")
		relCypher, relKeyParams := relPattern(event, "start", "rel", "end")
		state := event["state"].(map[string]any)
		relAttr, relParams := attribPattern(state, "rel")
		maps.Copy(startParams, endParams)
		maps.Copy(startParams, relKeyParams)
		maps.Copy(startParams, relParams)
		t := template.Must(template.New("createRelCypher").Parse(createRelCypher))
		if err := t.Execute(&tmp, map[string]string{
			"source":     startCypher,
			"target":     endCypher,
			"relPattern": relCypher,
			"rVar":       "rel",
			"attributes": relAttr,
		}); err != nil {
			return "", nil, err
		}
		return tmp.String(), startParams, nil

	} else if event["eventType"] == "n" {
		state := event["state"].(map[string]any)
		after := state["after"].(map[string]any)
		matchCypher, matchParams := nodePattern(event, "n")
		labelString := labelPattern(after, "n")
		attribString, attribParams := attribPattern(state, "n")
		maps.Copy(attribParams, matchParams)
		t := template.Must(template.New("createNodeCypher").Parse(createNodeCypher))
		if err := t.Execute(&tmp, map[string]string{
			"nodePattern": matchCypher,
			"nVar":        "n",
			"labels":      labelString,
			"attributes":  attribString,
		}); err != nil {
			return "", nil, err
		}
		return tmp.String(), attribParams, nil

	}

	return "", nil, nil
}

// createCypher merges a node on a set of parameters
// and updates its labels and attributes
const createNodeCypher = `
CREATE {{.nodePattern}}
SET {{.nVar}}{{.labels}}
SET {{.attributes}}
`

// createCypher merges a node on a set of parameters
// and updates its labels and attributes
const createRelCypher = `
MATCH {{.source}} 
MATCH {{.target}}
MERGE {{.relPattern}}
SET {{.attributes}}
`

type CDCService struct {
	source    neo4j.DriverWithContext
	sink      neo4j.DriverWithContext
	database  string
	waitGroup sync.WaitGroup
	cursor    atomic.Pointer[string]
	selectors []any
}

func (s *CDCService) queryChanges(ctx context.Context) error {
	session := s.source.NewSession(ctx, neo4j.SessionConfig{DatabaseName: s.database})

	_, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		result, err := tx.Run(ctx, "CALL cdc.query($from, $selectors)", map[string]any{
			"from":      s.from(),
			"selectors": s.selectors,
		})
		if err != nil {
			return "", err
		}

		var record *neo4j.Record
		for result.NextRecord(ctx, &record) {
			err := s.applyChange(ctx, record)
			if err != nil {
				return "", fmt.Errorf("error processing record: %w", err)
			}

			id, isNil, err := neo4j.GetRecordValue[string](record, "id")
			if err != nil || isNil {
				return "", fmt.Errorf("missing or invalid id value returned")
			}
			s.setFrom(id)
		}

		return s.from(), nil
	})
	if err != nil {
		return fmt.Errorf("unable to query/process changes: %w", err)
	}

	return nil
}

func (s *CDCService) earliestChangeID(ctx context.Context) (string, error) {
	return queryChangeID(ctx, s.source, s.database, "CALL cdc.earliest()")
}

func (s *CDCService) currentChangeID(ctx context.Context) (string, error) {
	return queryChangeID(ctx, s.source, s.database, "CALL cdc.current()")
}

func (s *CDCService) from() string {
	return *s.cursor.Load()
}

func (s *CDCService) setFrom(from string) {
	s.cursor.Store(&from)
}

func (s *CDCService) Start(ctx context.Context) error {
	if s.from() == "" {
		current, err := s.currentChangeID(ctx)
		if err != nil {
			return err
		}
		s.setFrom(current)
	}

	s.waitGroup.Add(1)
	go func(ctx context.Context) {
		defer func() {
			s.waitGroup.Done()
		}()

		timer := time.NewTimer(5 * time.Second)
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				{
					err := s.queryChanges(ctx)
					if err != nil {
						log.Printf("error querying/processing changes: %v", err)
						return
					}

					timer.Reset(500 * time.Millisecond)
				}
			}
		}
	}(ctx)

	return nil
}

func (s *CDCService) WaitForExit() {
	s.waitGroup.Wait()
}

func NewCDCService(source_address string, sink_address string, username string, password string, database string, from string, selectors []any) (*CDCService, error) {
	source, err := neo4j.NewDriverWithContext(source_address, neo4j.BasicAuth(username, password, ""))
	if err != nil {
		return nil, fmt.Errorf("unable to create driver: %w", err)
	}
	sink, err := neo4j.NewDriverWithContext(sink_address, neo4j.BasicAuth(username, password, ""))
	if err != nil {
		return nil, fmt.Errorf("unable to create driver: %w", err)
	}

	cdc := &CDCService{
		source:    source,
		sink:      sink,
		database:  database,
		waitGroup: sync.WaitGroup{},
		cursor:    atomic.Pointer[string]{},
		selectors: selectors,
	}
	cdc.setFrom(from)

	return cdc, nil
}

var (
	source_address string
	sink_address   string
	database       string
	username       string
	password       string
	from           string
)

func main() {
	rootCmd := &cobra.Command{
		Run: func(cmd *cobra.Command, args []string) {
			ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt)

			selectors := []any{
				//map[string]any{"select": "n", "labels": []string{"Person", "Employee"}},
			}

			cdc, err := NewCDCService(source_address, sink_address, username, password, database, from, selectors)
			if err != nil {
				log.Fatal(err)
			}

			if err := cdc.Start(ctx); err != nil {
				log.Fatal(err)
			}

			fmt.Printf("starting...\n")
			cdc.WaitForExit()
			fmt.Printf("quitting...\n")
		},
	}

	rootCmd.Flags().StringVarP(&source_address, "source_address", "o", "bolt://localhost:7687", "Bolt URI")
	rootCmd.Flags().StringVarP(&sink_address, "sink_address", "i", "bolt://localhost:7787", "Bolt URI")
	rootCmd.Flags().StringVarP(&database, "database", "d", "", "Database")
	rootCmd.Flags().StringVarP(&username, "username", "u", "neo4j", "Username")
	rootCmd.Flags().StringVarP(&password, "password", "p", "password", "Password")
	rootCmd.Flags().StringVarP(&from, "from", "f", "", "Change identifier to query changes from")

	cobra.CheckErr(rootCmd.Execute())
}
