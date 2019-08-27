//  BitWrk - A Bitcoin-friendly, anonymous marketplace for computing power
//  Copyright (C) 2013-2019  Jonas Eschenburg <jonas@bitwrk.net>
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
	"encoding/hex"
	"encoding/json"
)

var _ json.Marshaler = SKey{}
var _ json.Unmarshaler = &SKey{}

func (k SKey) MarshalJSON() ([]byte, error) {
	l := hex.EncodedLen(len(k)) + 2
	dst := make([]byte, l)
	dst[0] = '"'
	dst[l-1] = '"'
	hex.Encode(dst[1:l-1], k[:])
	return dst, nil
}

func (k *SKey) UnmarshalJSON(b []byte) error {
	t := []byte(k[:])
	return json.Unmarshal(b, &t)
}
