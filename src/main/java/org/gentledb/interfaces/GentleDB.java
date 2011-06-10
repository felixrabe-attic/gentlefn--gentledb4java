/*
 * Copyright (C) 2011  Felix Rabe  (www.felixrabe.net)
 *
 * GentleDB is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * GentleDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with GentleDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.gentledb.interfaces;

import org.gentledb.Utilities.GentleDBException;

public interface GentleDB {

	public OutputStream addStream() throws GentleDBException;
	public InputStream getStream(String contentId) throws GentleDBException;
	
	public String addArray(byte[] content) throws GentleDBException;
	public byte[] getArray(String contentId) throws GentleDBException;
	
	public String addString(String content) throws GentleDBException;
	public String getString(String contentId) throws GentleDBException;
	
	public void put(String pointerId, String contentId) throws GentleDBException;
	public String get(String pointerId) throws GentleDBException;
	
}
