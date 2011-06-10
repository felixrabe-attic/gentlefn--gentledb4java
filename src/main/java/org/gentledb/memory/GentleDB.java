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
package org.gentledb.memory;

import static org.gentledb.Utilities.sha256;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.gentledb.Utilities;
import org.gentledb.Utilities.GentleDBException;

public class GentleDB implements org.gentledb.interfaces.GentleDB {

	private Map<String, byte[]> contentDB;
	private Map<String, String> pointerDB;
	
	public GentleDB() throws GentleDBException {
        contentDB = new HashMap<String, byte[]>();
        pointerDB = new HashMap<String, String>();
	}
	
	private String[] findPartialId(File directory, String partialId) {
		// TODO
		return null;
	}
	
	public OutputStream addStream() throws GentleDBException {
		return new OutputStream(this);
	}

	public InputStream getStream(String contentId) throws GentleDBException {
		return new InputStream(this, contentId);
	}

	public String addArray(byte[] content) throws GentleDBException {
		final OutputStream stream = addStream();
		try {
			IOUtils.write(content, stream);
		} catch (IOException e) {
			throw new GentleDBException(e);
		} finally {
			IOUtils.closeQuietly(stream);
		}
		final String contentId = stream.getContentId();
		return contentId;
	}

	public byte[] getArray(String contentId) throws GentleDBException {
		Utilities.validateIdentifier(contentId);
		final InputStream stream = getStream(contentId);
		try {
			return IOUtils.toByteArray(stream);
		} catch (IOException e) {
			throw new GentleDBException(e);
		} finally {
			IOUtils.closeQuietly(stream);
		}
	}

	@Override
	public String addString(String content) throws GentleDBException {
		try {
			return addArray(content.getBytes("utf-8"));
		} catch (UnsupportedEncodingException e) {
			throw new GentleDBException(e);
		}
	}

	@Override
	public String getString(String contentId) throws GentleDBException {
		try {
			return new String(getArray(contentId), "utf-8");
		} catch (UnsupportedEncodingException e) {
			throw new GentleDBException(e);
		}
	}
	
	@Override
	public void put(String pointerId, String contentId) throws GentleDBException {
		Utilities.validateIdentifier(pointerId);
		if (contentId != null) {
			Utilities.validateIdentifier(contentId);
			pointerDB.put(pointerId, contentId);
		} else {  // contentId == null, so remove the pointer
			pointerDB.remove(pointerId);
		}
	}

	@Override
	public String get(String pointerId) throws GentleDBException {
		Utilities.validateIdentifier(pointerId);
		return pointerDB.get(pointerId);
	}



	public static class OutputStream extends java.io.OutputStream implements org.gentledb.interfaces.OutputStream {
		
		private GentleDB db;
		private MessageDigest sha256;
		private String contentId;
		private byte[] content;
		private ByteArrayOutputStream stream;
		private boolean isOpen;
		
		public OutputStream(GentleDB db) throws GentleDBException {
			super();
			this.db = db;
			sha256 = sha256();
			stream = new ByteArrayOutputStream();
			isOpen = true;
		}

		@Override
		public void write(int b) throws IOException {
			sha256.update((byte) b);
			stream.write(b);
		}

		@Override
		public void write(byte[] b) throws IOException {
			sha256.update(b);  // sha256.digest();
			stream.write(b);
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			sha256.update(b, off, len);
			stream.write(b, off, len);
		}

		@Override
		public void close() throws IOException {
			if (!isOpen)
				return;
			IOUtils.closeQuietly(stream);
			isOpen = false;
			String contentId;
			try {
				contentId = getContentId();
			} catch (GentleDBException e) {
				throw new IOException(e);
			}
			db.contentDB.put(contentId, content);
		}
		
		public String getContentId() throws GentleDBException {
			if (isOpen) {
				try {
					close();
				} catch (IOException e) {
					throw new GentleDBException(e);
				}
			}
			if (contentId == null) {
				byte[] digest = sha256.digest();
				contentId = Utilities.hex(digest);
				content = stream.toByteArray();
			}
			return contentId;
		}
	}
	
	public class InputStream extends java.io.InputStream implements org.gentledb.interfaces.InputStream {
		
		private ByteArrayInputStream stream;
		
		public InputStream(GentleDB db, String contentId) throws GentleDBException {
			super();
			stream = new ByteArrayInputStream(db.contentDB.get(contentId));
		}

		@Override
		public int read() throws IOException {
			return stream.read();
		}

		@Override
		public int read(byte[] b) throws IOException {
			return stream.read(b);
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			return stream.read(b, off, len);
		}
	}
	
}
