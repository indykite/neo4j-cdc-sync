package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/spf13/cobra"
	"github.com/tidwall/pretty"
)

func applyChange(record *neo4j.Record) error {
	jsonOutput, err := json.Marshal(asMap(record))
	if err != nil {
		return fmt.Errorf("unable to jsonify record: %w", err)
	}

	fmt.Println(string(pretty.Color(pretty.Pretty(jsonOutput), pretty.TerminalStyle)))

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

type CDCService struct {
	driver    neo4j.DriverWithContext
	database  string
	waitGroup sync.WaitGroup
	cursor    atomic.Pointer[string]
	selectors []any
}

func (s *CDCService) queryChanges(ctx context.Context) error {
	session := s.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: s.database})

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
			err := applyChange(record)
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
	return queryChangeID(ctx, s.driver, s.database, "CALL cdc.earliest()")
}

func (s *CDCService) currentChangeID(ctx context.Context) (string, error) {
	return queryChangeID(ctx, s.driver, s.database, "CALL cdc.current()")
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

		timer := time.NewTimer(0 * time.Millisecond)
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

func NewCDCService(uri string, username string, password string, database string, from string, selectors []any) (*CDCService, error) {
	driver, err := neo4j.NewDriverWithContext(uri, neo4j.BasicAuth(username, password, ""))
	if err != nil {
		return nil, fmt.Errorf("unable to create driver: %w", err)
	}

	cdc := &CDCService{
		driver:    driver,
		database:  database,
		waitGroup: sync.WaitGroup{},
		cursor:    atomic.Pointer[string]{},
		selectors: selectors,
	}
	cdc.setFrom(from)

	return cdc, nil
}

var (
	address  string
	database string
	username string
	password string
	from     string
)

func main() {
	rootCmd := &cobra.Command{
		Run: func(cmd *cobra.Command, args []string) {
			ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt)

			selectors := []any{
				//map[string]any{"select": "n", "labels": []string{"Person", "Employee"}},
			}

			cdc, err := NewCDCService(address, username, password, database, from, selectors)
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

	rootCmd.Flags().StringVarP(&address, "address", "a", "bolt://localhost:7687", "Bolt URI")
	rootCmd.Flags().StringVarP(&database, "database", "d", "", "Database")
	rootCmd.Flags().StringVarP(&username, "username", "u", "neo4j", "Username")
	rootCmd.Flags().StringVarP(&password, "password", "p", "password", "Password")
	rootCmd.Flags().StringVarP(&from, "from", "f", "", "Change identifier to query changes from")

	cobra.CheckErr(rootCmd.Execute())
}
