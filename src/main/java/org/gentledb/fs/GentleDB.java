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
package org.gentledb.fs;

import static org.gentledb.Utilities.sha256;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.gentledb.Utilities;
import org.gentledb.Utilities.GentleDBException;

public class GentleDB implements org.gentledb.interfaces.GentleDB {
	
	private File directory;
	private File contentDir;
	private File pointerDir;
	private File tmpDir;
	
	public GentleDB(File directory) throws GentleDBException {
		try {
			this.directory = directory.getCanonicalFile();
		} catch (IOException e) {
			throw new GentleDBException(e);
		}
		contentDir = new File(this.directory, "content_db");
		pointerDir = new File(this.directory, "pointer_db");
		tmpDir = new File(this.directory, "tmp");
		for (File dir : new File[] { this.directory, contentDir, pointerDir, tmpDir }) {
			if (!dir.exists()) {
				Utilities.mkdir700(dir);
			}
		}
		
	}
	
	public GentleDB(String directory) throws GentleDBException {
		this(new File(directory));
	}
	
	public GentleDB() throws GentleDBException {
		this(new File(new File(System.getProperty("user.home")), ".gentledb"));
	}
	
	public File getDirectory() {
		return directory;
	}
	
	private static File idToPath(File directory, String id, boolean createDir) throws GentleDBException {
		final Matcher matcher = Pattern.compile("(..?)(.?.?)(.?.?.?)(.*)").matcher(id);
		if (!matcher.matches()) {
			throw new GentleDBException("Invalid identifier: '" + id + "'");
		}
		for (int i = 1; i < matcher.groupCount(); i++) {
			if (matcher.group(i).length() == 0)
				break;
			directory = new File(directory, matcher.group(i));
			if (createDir && !directory.exists()) {
				Utilities.mkdir700(directory);
			}
		}
		return new File(directory, matcher.group(matcher.groupCount()));
	}
	
	private File getContentFile(String contentId, boolean createDir) throws GentleDBException {
		return idToPath(contentDir, contentId, createDir);
	}
	
	private File getPointerFile(String pointerId, boolean createDir) throws GentleDBException {
		return idToPath(pointerDir, pointerId, createDir);
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
			File file = getPointerFile(pointerId, true);
			FileOutputStream stream;
			try {
				stream = new FileOutputStream(file);
			} catch (FileNotFoundException e) {
				throw new GentleDBException(e);
			}
			// chmod 0600
			file.setReadable(false, false);
			file.setReadable(true, true);
			file.setWritable(false, false);
			file.setWritable(true, true);
			file.setExecutable(false, false);
			try {
				IOUtils.write(contentId, stream, "utf-8");
			} catch (IOException e) {
				throw new GentleDBException(e);
			} finally {
				IOUtils.closeQuietly(stream);
			}
		} else {  // contentId == null, so remove the pointer
			File file = getPointerFile(pointerId, false);
			if (file.exists()) {
				if (!file.delete()) {
					String filePath;
					try {
						filePath = file.getCanonicalPath();
					} catch (IOException e) {
						filePath = file.getPath();
					}
					throw new GentleDBException("Could not delete '" + filePath + "'");
				}
			}
		}
	}

	@Override
	public String get(String pointerId) throws GentleDBException {
		Utilities.validateIdentifier(pointerId);
		FileInputStream stream;
		try {
			stream = new FileInputStream(getPointerFile(pointerId, false));
		} catch (FileNotFoundException e) {
			throw new GentleDBException(e);
		}
		String contentId;
		try {
			contentId = IOUtils.toString(stream, "utf-8");
		} catch (IOException e) {
			throw new GentleDBException(e);
		} finally {
			IOUtils.closeQuietly(stream);
		}
		return contentId;
	}



	public static class OutputStream extends java.io.OutputStream implements org.gentledb.interfaces.OutputStream {
		
		private GentleDB db;
		private MessageDigest sha256;
		private String contentId;
		private File streamFile;
		private FileOutputStream stream;
		private boolean isOpen;
		
		public OutputStream(GentleDB db) throws GentleDBException {
			super();
			this.db = db;
			sha256 = sha256();
			streamFile = new File(db.tmpDir, Utilities.random());
			try {
				stream = new FileOutputStream(streamFile);
			} catch (FileNotFoundException e) {
				throw new GentleDBException(e);
			}
			// chmod 0600
			streamFile.setReadable(false, false);
			streamFile.setReadable(true, true);
			streamFile.setWritable(false, false);
			streamFile.setWritable(true, true);
			streamFile.setExecutable(false, false);
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
			File file;
			try {
				file = db.getContentFile(contentId, true);
			} catch (GentleDBException e) {
				throw new IOException(e);
			}
			if (!file.exists()) {
				streamFile.setWritable(false, false);  // chmod 0400
				streamFile.renameTo(file);
			} else {  // we do not overwrite existing content
				if (!streamFile.delete()) {
					String filePath;
					try {
						filePath = streamFile.getCanonicalPath();
					} catch (IOException e) {
						filePath = streamFile.getPath();
					}
					throw new IOException("Could not delete '" + filePath + "'");
				}
			}
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
			}
			return contentId;
		}
	}
	
	public class InputStream extends java.io.InputStream implements org.gentledb.interfaces.InputStream {
		
		private FileInputStream stream;
		
		public InputStream(GentleDB db, String contentId) throws GentleDBException {
			super();
			try {
				stream = new FileInputStream(db.getContentFile(contentId, false));
			} catch (FileNotFoundException e) {
				throw new GentleDBException(e);
			}
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
