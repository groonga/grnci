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
// Note that C.grn_init must not be called if Groonga is already initialized.
//
// Grnci, by default, initializes Groonga when it creates the first DB
// instance and finalizes Groonga when it closes the last DB instance.
// To achieve this, Grnci uses a reference count.

// grnCnt is a reference count for Groonga.
// grnInit increments grnCnt and grnFin decrements grnCnt.
var grnCnt uint32

// grnCntMutex is a mutex for grnCnt.
var grnCntMutex sync.Mutex

// grnInitLib initializes Groonga.
func grnInitLib() error {
	if rc := C.grn_init(); rc != C.GRN_SUCCESS {
		return fmt.Errorf("grn_init failed: rc = %s", rc)
	}
	C.grnci_init_thread_limit()
	return nil
}

// grnFinLib finalizes Groonga.
func grnFinLib() error {
	if rc := C.grn_fin(); rc != C.GRN_SUCCESS {
		return fmt.Errorf("grn_fin failed: rc = %s", rc)
	}
	return nil
}

// grnInit increments grnCnt and initializes Groonga if it changes from 0 to 1.
func grnInit() error {
	grnCntMutex.Lock()
	defer grnCntMutex.Unlock()
	if grnCnt == math.MaxUint32 {
		return fmt.Errorf("grnCnt = %d", grnCnt)
	}
	if grnCnt == 0 {
		if err := grnInitLib(); err != nil {
			return err
		}
	}
	grnCnt++
	return nil
}

// grnFin decrements grnCnt and finalizes Groonga if it changes from 1 to 0.
func grnFin() error {
	grnCntMutex.Lock()
	defer grnCntMutex.Unlock()
	if grnCnt == 0 {
		return fmt.Errorf("grnCnt = %d", grnCnt)
	}
	grnCnt--
	if grnCnt == 0 {
		if err := grnFinLib(); err != nil {
			return err
		}
	}
	return nil
}

// GrnInit explicitly initializes Groonga.
// GrnInit should not be called if Groonga is already initialized.
//
// Note that Groonga is implicitly initialized when Grnci creates the first DB
// instance.
func GrnInit() error {
	grnCntMutex.Lock()
	defer grnCntMutex.Unlock()
	if grnCnt != 0 {
		return fmt.Errorf("grnCnt = %d", grnCnt)
	}
	if err := grnInitLib(); err != nil {
		return err
	}
	grnCnt++
	return nil
}

// GrnFin explicitly finalizes Groonga.
// GrnFin should be used if Groonga is initialized by GrnInit.
//
// Note that Groonga is implicitly finalized when Grnci closes the last DB
// instance if GrnInit is not used.
func GrnFin() error {
	grnCntMutex.Lock()
	defer grnCntMutex.Unlock()
	if grnCnt == 0 {
		return fmt.Errorf("grnCnt = %d", grnCnt)
	} else if grnCnt != 1 {
		return fmt.Errorf("grnCnt = %d", grnCnt)
	}
	grnCnt--
	return grnFinLib()
}

// DisableGrnInit disables implicit Groonga initialization and finalization.
// DisableGrnInit should be called before creating the first DB instance.
func DisableGrnInit() error {
	grnCntMutex.Lock()
	defer grnCntMutex.Unlock()
	if grnCnt != 0 {
		return fmt.Errorf("grnCnt = %d", grnCnt)
	}
	grnCnt++
	return nil
}
