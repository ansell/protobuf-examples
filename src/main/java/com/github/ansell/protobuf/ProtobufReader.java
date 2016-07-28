/*
 * Copyright 2016 Peter Ansell
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.github.ansell.protobuf;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.protobuf.ProtobufMapper;
import com.fasterxml.jackson.dataformat.protobuf.schema.ProtobufSchema;
import com.fasterxml.jackson.dataformat.protobuf.schema.ProtobufSchemaLoader;
import com.github.ansell.csv.util.CSVUtil;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * Protobuf reader application.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class ProtobufReader {

	public static void main(String... args)
		throws Exception
	{
		final OptionParser parser = new OptionParser();

		final OptionSpec<Void> help = parser.accepts("help").forHelp();
		final OptionSpec<File> input = parser.accepts("input").withRequiredArg().ofType(
				File.class).required().describedAs("The input protobuf file.");
		final OptionSpec<File> schema = parser.accepts("schema").withRequiredArg().ofType(
				File.class).required().describedAs("The protobuf schema.");
		final OptionSpec<File> output = parser.accepts("output").withRequiredArg().ofType(
				File.class).required().describedAs("The output protobuf file.");
		final OptionSpec<String> outputFormat = parser.accepts("output-format").withRequiredArg().ofType(
				String.class).required().describedAs("The output format, currently only supporting CSV.");
		final OptionSpec<Boolean> debug = parser.accepts("debug").withOptionalArg().ofType(
				Boolean.class).defaultsTo(Boolean.FALSE).describedAs(
						"Set to true to debug the protobuf operations");

		OptionSet options = null;

		try {
			options = parser.parse(args);
		}
		catch (final OptionException e) {
			System.out.println(e.getMessage());
			parser.printHelpOn(System.out);
			throw e;
		}

		if (options.has(help)) {
			parser.printHelpOn(System.out);
			return;
		}

		final Path inputPath = input.value(options).toPath();
		if (!Files.exists(inputPath)) {
			throw new FileNotFoundException("Could not find input protobuf file: " + inputPath.toString());
		}

		final Path schemaPath = schema.value(options).toPath();
		if (!Files.exists(schemaPath)) {
			throw new FileNotFoundException("Could not find protobuf schema file: " + schemaPath.toString());
		}

		final Path outputPath = input.value(options).toPath();
		if (Files.exists(outputPath)) {
			throw new FileNotFoundException("Output path already exists: " + outputPath.toString());
		}

		try (final BufferedReader schemaReader = Files.newBufferedReader(schemaPath);) {
			ProtobufSchema schemaObject = ProtobufSchemaLoader.std.load(schemaReader);
			try (final InputStream inputProtobuf = Files.newInputStream(inputPath);
					final BufferedWriter outputStream = Files.newBufferedWriter(outputPath);)
			{

				ObjectMapper mapper = new ProtobufMapper();
				final MappingIterator<List<String>> it = mapper.readerFor(List.class).with(
						schemaObject).readValues(inputProtobuf);
				List<String> header = null;
				SequenceWriter csvWriter = null;
				try {
					while (it.hasNext()) {
						List<String> nextLine = it.next();
						if (header == null) {
							header = nextLine;
							csvWriter = CSVUtil.newCSVWriter(outputStream, header);
						}
						else {
							csvWriter.write(nextLine);
						}
					}
				}
				finally {
					if (csvWriter != null) {
						csvWriter.close();
					}
				}
			}
		}

	}

}
