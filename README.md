Token Replacement Plugin
===

The Token Replacement Plugin for Pentaho Data Integration allows you to replace tokens in strings or files.  This plugin can read the string to replace from text entered into the step, a field on the stream, or even a file.  The step can output the result to a string field or to a file.

A big thank you to my employer [Inquidia Consulting](www.inquidia.com) for allowing me to open source this plugin.

System Requirements
---
-Pentaho Data Integration 6.0 or above (Plugin version 2.0 and above)
-Pentaho Data Integration 5.0 or above (Plugin Version 1.x and below)

Installation
---
**Using Pentaho Marketplace**

1. In the Pentaho Marketplace find the Token Replacement plugin and click Install
2. Restart Spoon

**Manual Install**

1. Place the TokenReplacementPlugin folder in the ${DI\_HOME}/plugins/steps directory
2. Restart Spoon

Usage
---
This step replaces tokens in an input string or file.  The step can then output this data either to a file or a field on the stream.

A token contains a start string, a name, and an end string.  For example ${my_token} could be a token.  The start string, and end string are configurable and can be any series of characters.

When replacing tokens in a file, this step reads the entire file.  It does not operate on a single line at a time.  When replacing tokens in a file it is a best practice to output to a file also to prevent Pentaho from having to read the entire file into memory.

**Input Tab**
* Input type - Where to read the field to do the token replacement from.  Either text, field, or file.

*Input Type Text*
* Input Text - The text to token replace

*Input Type Field*
* Input Field - The input field to token replace

*Input Type File*
* Input filename - The name of the file to token replace
* Filename is in field? - Is the name of the file to token replace in a field?
* Input filename field - The field the name of the file is in.
* Add Input filename to result? - Add the input filename(s) to the result files list.

**Output Tab**
* Output Type - Where to put the token replaced string.  Either field or file.

*Output Type Field*
* Output field name - The name of the field to put the token replaced string in.

*Output Type File*
* Output filename - The name of the file to write to.
* Filename is in field? - Is the name of the output file in a field?
* Output filename field - The name of the field the output filename is in.
* Append output file? - If the output file already exists should it be appended to.  If not checked, Pentaho will overwrite the file if it exists.
* Create parent folder? - Should Pentaho create the parent folder?
* Output format - The format of the new line delimiter for the output file.
* Output encoding - The character encoding to use when writing the file.
* Split every - Split the output file into a new file every n rows.
* Include stepnr in filename? - Should the step number be included in the output filename?
* Include partition nr in filename? - Should the partition number be included in the output filename?
* Include date in filename? - Should the curren date be included in the output filename?
* Include time in filename? - Should the current time be included in the output filename?
* Specify date format? - Do you want to specify the date format to include in the output filename?
* Date time format - The date/time format to include in the output filename.
* Add output filenames to result? - Add the output filename(s) to the result files list.

**Tokens Tab**
* Token start string - The string that indicates the start of a token.
* Token end string - The string that indicates the end of a token.  Everything between the token start string and the token end string is the token name.
* Stream name - The name of the field on the stream containing the value to replace the token with.
* Token name - The name of the token to replace.
* Get Fields button - Gets the list of input fields, and tries to map them to an Avro field by an exact name match.

Building from Source
---
The Token Replacement Plugin is built using Ant.  Since I do not want to deal with the complexities of Ivy the following instructions must be followed before building this plugin.

1. Edit the build.properties file setting the Pentaho and Java version.
5. Run "ant clean resolve dist" to build the plugin.
