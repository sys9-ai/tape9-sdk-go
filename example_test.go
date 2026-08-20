package tape9_test

import (
	"bytes"
	"context"
	"io"

	tape9 "github.com/sys9-ai/tape9-sdk-go"
)

func ExampleNew() {
	client, err := tape9.New("https://tape9.example", tape9.WithSecret("space-secret"))
	if err != nil {
		panic(err)
	}
	_ = client
}

func ExampleClient_Append() {
	client, err := tape9.New("https://tape9.example", tape9.WithSecret("space-secret"))
	if err != nil {
		panic(err)
	}

	_, _ = client.Append(
		context.Background(),
		"space-id",
		"tape-id",
		bytes.NewBufferString("payload"),
		tape9.AppendOptions{Compression: tape9.CompressionZstd},
	)
}

func ExampleClient_Read() {
	client, err := tape9.New("https://tape9.example", tape9.WithSecret("space-secret"))
	if err != nil {
		panic(err)
	}

	_, _ = client.Read(context.Background(), "space-id", "tape-id", io.Discard)
}
