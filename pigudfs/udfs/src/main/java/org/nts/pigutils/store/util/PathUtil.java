package org.nts.pigutils.store.util;


import java.io.IOException;
import java.util.Collection;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 
 * Provides basic helper functionality to the AllLoader.
 * 
 */
public class PathUtil {
	/**
	 * Used to know if a path is a glob pattern
	 */
	private static final Pattern globPattern = Pattern.compile("[?*\\[\\{]");

	/**
	 * Returns true if glob
	 * 
	 * @param file
	 * @return boolean
	 */
	public static final boolean isGlob(String file) {
		return globPattern.matcher(file).find();
	}

	/**
	 * Is a quick function helper that will list recursively.<br/>
	 * Supports globs.
	 * 
	 * @param fileStatusFiles
	 *            Collection FileStatus, all FileStatus items are added to this
	 *            collection
	 * @throws IOException
	 */
	public static void listRecursive(Path path, Configuration conf,
			Collection<FileStatus> fileStatusFiles) throws IOException {

		FileSystem fs = path.getFileSystem(conf);

		if (isGlob(path.toUri().getPath())) {
			// the path is a glob
			FileStatus[] statArr = fs.globStatus(path);

			if (statArr == null) {
				return;
			}

			for (FileStatus subStat : statArr) {
				listRecursive(subStat, fs, fileStatusFiles);
			}

		} else {
			// no glob

			FileStatus stat = fs.getFileStatus(path);
			if (stat == null) {
				return;
			}

			if (stat.isDirectory()) {

				// check for glob status

				// we iterate through the directory's children
				FileStatus[] statArr = fs.listStatus(path);
				for (FileStatus subStat : statArr) {
					listRecursive(subStat, fs, fileStatusFiles);
				}

			} else {
				fileStatusFiles.add(stat);
			}
		}

	}

	/**
	 * Recursively iterate through a directory and find all files
	 * 
	 * @param fileStatus
	 * @param fs
	 * @param fileStatusFiles
	 * @throws IOException
	 */
	public static void listRecursive(FileStatus fileStatus, FileSystem fs,
			Collection<FileStatus> fileStatusFiles) throws IOException {

		Path path = fileStatus.getPath();

		// filter out hidden files
		if (path.getName().startsWith("_")) {
			return;
		}

		// if a directory, we iterate through all the sub directories
		if (fileStatus.isDirectory()) {
			FileStatus[] statArr = fs.listStatus(path);
			for (FileStatus subStat : statArr) {
				listRecursive(subStat, fs, fileStatusFiles);
			}
		} else {
			// if a file, add to the file status list
			fileStatusFiles.add(fileStatus);
		}

	}

}