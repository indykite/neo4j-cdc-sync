/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"

	uuidLib "github.com/google/uuid"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/spf13/cobra"
)

type Node struct {
	id     string
	labels []string
}

type Relationship struct {
	id      string
	relType string
}
type Params struct {
	source Node
	target Node
	rel    Relationship
}

const query = `
UNWIND $triples AS triple
MERGE (s:Unique:Resource {id: triple.source, external_id: triple.source, type:"Car"})
MERGE (t:Mouse:Unique:DigitalTwin {id: triple.target, external_id: triple.target, type: "Rodent"})
MERGE (s)-[:RELATES_TO {id: 123}]->(t)
`

func generateUUID(seenUUIDs map[string]struct{}) string {
	uuid := uuidLib.New().String()
	for _, ok := seenUUIDs[uuid]; ok; {
		uuid = uuidLib.New().String()
	}
	return uuid
}

var labels []string = []string{"Cat", "Mouse", "Dog", "Car", "Home"}

func getRandomLabels(n int) []string {
	if n > len(labels) {
		panic("Asked for more labels than there are in the labels array")
	}
	if n == len(labels) {
		return labels
	}
	ret := make([]string, n)
	sample := rand.Perm(len(labels))
	for i := 0; i < n; i++ {
		ret = append(ret, labels[sample[i]])
	}
	return ret
}

func generateData(n int) []map[string]string {
	triples := make([]map[string]string, 0, n)
	uuidSet := make(map[string]struct{}, n)
	for i := 0; i < n; i++ {
		n := rand.Intn(2)
		source := Node{
			id:     generateUUID(uuidSet),
			labels: getRandomLabels(n),
		}
		target := Node{
			id:     generateUUID(uuidSet),
			labels: getRandomLabels(n),
		}
		triples = append(triples, map[string]string{
			"source": generateUUID(uuidSet),
			"target": generateUUID(uuidSet),
			"edge":   generateUUID(uuidSet),
		})
	}
	return triples
}

// seedCmd represents the seed command
var seedCmd = &cobra.Command{
	Use:   "seed [number of triples to create]",
	Short: "Seeds the source database with nodes and relationships.",
	Long:  `Creates a number of triples of the type (:Resource)-[:RELATES_TO]->(:Resource) in the source database.`,
	Args:  cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		var (
			n      int
			driver neo4j.DriverWithContext
		)
		username := "neo4j"
		password := "password"
		uri := "neo4j://localhost:7687"
		arg := args[0]
		n, err := strconv.Atoi(arg)
		if err != nil {
			return errors.New("Argument passed to seed must be a number")
		}
		triples := generateData(n)
		driver, err = neo4j.NewDriverWithContext(uri, neo4j.BasicAuth(username, password, ""))
		if err != nil {
			return fmt.Errorf("unable to create driver: %w", err)
		}
		ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt)
		session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
		defer session.Close(ctx)
		_, err = session.ExecuteWrite(ctx, func(transaction neo4j.ManagedTransaction) (any, error) {
			result, err := transaction.Run(ctx, query, map[string]any{"triples": triples})
			if err != nil {
				return nil, err
			}
			return nil, result.Err()
		})
		if err != nil {
			return err
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(seedCmd)
	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// seedCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// seedCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
