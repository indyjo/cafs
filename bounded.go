//  BitWrk - A Bitcoin-friendly, anonymous marketplace for computing power
//  Copyright (C) 2013-2018  Jonas Eschenburg <jonas@bitwrk.net>
//
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
//
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cafs

import (
	"fmt"
)

// Type UsageInfo contains information about how many bytes are used, locked and available
// by a BoundedStorage.
type UsageInfo struct {
	Used     int64 // The number of bytes used by the storage
	Capacity int64 // The maximum number of bytes usable by the storage
	Locked   int64 // The number of bytes currently locked by the storage
}

func (ui UsageInfo) String() string {
	return fmt.Sprintf("%d of %d kb used with %d kb locked", kb(ui.Used), kb(ui.Capacity), kb(ui.Locked))
}

func kb(v int64) int64 {
	return (v + 1023) >> 10
}

// Interface BoundedStorage describes file storage with bounded capacity
type BoundedStorage interface {
	FileStorage

	GetUsageInfo() UsageInfo

	// Clears any data that is not locked externally and returns the number of bytes freed.
	FreeCache() int64
}
