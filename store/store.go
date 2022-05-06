// Copyright (c) 2022, Nicolas Thauvin All rights reserved.
// Use of this source code is governed by a BSD-style license that can be found
// in the LICENSE file.

package store

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"
)

type StoreData struct {
	Id int
	Ts time.Time
}

type Store struct {
	Path        string
	fd          *os.File
	count       int
	decodeError error
	decodedData StoreData
	decoder     *json.Decoder
}

func NewStore(path string, truncate bool) (*Store, error) {
	s := Store{Path: path}

	flags := os.O_RDWR | os.O_CREATE
	if truncate {
		flags = flags | os.O_TRUNC
	}

	f, err := os.OpenFile(path, flags, 0644)
	if err != nil {
		return nil, fmt.Errorf("could not open file: %w", err)
	}

	s.fd = f

	return &s, nil
}

func (s *Store) Close() error {
	return s.fd.Close()
}

func (s *Store) Append(id int, ts time.Time) error {
	data := StoreData{
		Id: id,
		Ts: ts,
	}

	_, err := s.fd.Seek(0, os.SEEK_END)
	if err != nil {
		return fmt.Errorf("could not seek to the end of the store: %w", err)
	}

	enc := json.NewEncoder(s.fd)
	if err := enc.Encode(data); err != nil {
		return fmt.Errorf("could not encode id: %w", err)
	}

	return nil
}

//
func (s *Store) Next() bool {
	if s.decoder == nil {
		s.decoder = json.NewDecoder(s.fd)
	}

	datum := StoreData{}

	if err := s.decoder.Decode(&datum); err != nil {
		if err != io.EOF {
			s.decodeError = fmt.Errorf("decode error: %w (%d)", err, s.count)
		}
		return false
	}

	s.decodedData = datum
	s.count++

	return true
}

func (s *Store) Values() ([]interface{}, error) {
	// Put the data found by Next() in a list in the correct order
	values := make([]interface{}, 0)

	values = append(values, s.decodedData.Id)
	values = append(values, s.decodedData.Ts)

	return values, nil
}

func (s *Store) Err() error {
	return s.decodeError
}
