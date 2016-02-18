package grnci

// #cgo pkg-config: groonga
// #include <groonga.h>
// #include <stdlib.h>
// #include "grnci.h"
import "C"

import (
	"fmt"
	"math"
	"sync"
)

// This source file provides variables and functions to initialize and finalize
// Groonga.
//
// C.grn_init initializes Groonga and C.grn_fin finalizes Groonga.
// Note that C.grn_init() must not be called if Groonga is already initialized.
//
// Grnci automatically initializes Groonga when it creates a new DB instance.
// To achieve this, Grnci uses a reference count.

// grnCnt is a reference count for Groonga.
// grnInit increments grnCnt and grnFin decrements grnCnt.
var grnCnt uint32

// grnCntMutex is a mutex for grnCnt.
var grnCntMutex sync.Mutex

// grnInit increments grnCnt and initializes Groonga if grnCnt changes from 0
// to 1.
func grnInit() error {
	grnCntMutex.Lock()
	defer grnCntMutex.Unlock()
	if grnCnt == math.MaxUint32 {
		return fmt.Errorf("grnCnt overflow")
	}
	if grnCnt == 0 {
		if rc := C.grn_init(); rc != C.GRN_SUCCESS {
			return fmt.Errorf("grn_init() failed: rc = %s", rc)
		}
		C.grnci_init_thread_limit()
	}
	grnCnt++
	return nil
}

// grnFin decrements grnCnt and initializes Groonga if grnCnt changes from 1
// to 0.
func grnFin() error {
	grnCntMutex.Lock()
	defer grnCntMutex.Unlock()
	if grnCnt == 0 {
		return fmt.Errorf("grnCnt underflow")
	}
	grnCnt--
	if grnCnt == 0 {
		if rc := C.grn_fin(); rc != C.GRN_SUCCESS {
			return fmt.Errorf("grn_fin() failed: rc = %s", rc)
		}
	}
	return nil
}
