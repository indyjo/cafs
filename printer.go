//  BitWrk - A Bitcoin-friendly, anonymous marketplace for computing power
//  Copyright (C) 2013-2017  Jonas Eschenburg <jonas@bitwrk.net>
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
	"io"
)

// Interface Printer is used by CAFS for debugging output
type Printer interface {
	Printf(format string, v ...interface{})
}

type writerPrinter struct {
	w io.Writer
}

func NewWriterPrinter(w io.Writer) Printer {
	return writerPrinter{w}
}

func (p writerPrinter) Printf(format string, v ...interface{}) {
	if len(format) == 0 || format[len(format)-1] != '\n' {
		format = format + "\n"
	}
	fmt.Fprintf(p.w, format, v...)
}
