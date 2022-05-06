// Copyright (c) 2022, Nicolas Thauvin All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

package main

import (
	"context"
	"github.com/orgrim/pg_happy/cmd"
	"os"
)

func main() {
	ctx := context.Background()

	if err := cmd.Execute(ctx); err != nil {
		os.Exit(1)
	}
}
