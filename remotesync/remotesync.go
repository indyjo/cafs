//  BitWrk - A Bitcoin-friendly, anonymous marketplace for computing power
//  Copyright (C) 2013-2018 Jonas Eschenburg <jonas@bitwrk.net>
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

// Package remotesync implements a differential file synching mechanism based on the content-based chunking
// that is used by CAFS internally.
// Step 1: Sender and receiver agree on hashes of the file's chunks
// Step 2: Receiver streams missing chunks (one bit per chunk)
// Step 3: Sender responds by sending content of requested chunks
package remotesync

var LoggingEnabled = false
