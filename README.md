# Tapsoob

Tapsoob is a simple tool to export and import databases. It is inspired by <https://github.com/ricardochimal/taps> but instead of having to rely on a server and a client, databases are exported to the filesystem or imported from a previous export, hence the OOB (out-of-band). This was done in order to avoid exposing a whole database using the HyperText Protocol for security reasons.

Although the code is not quite perfect yet and some improvement will probably be made in the future it's usable. However if you notice an issue don't hesitate to report it.


## Database support

Tapsoob currently rely on the Sequel ORM (<http://sequel.rubyforge.org/>) so we currently support every database that Sequel supports. If additionnal support is required in the future (NoSQL databases) I'll make my best to figure out a way to bring that in my ToDo list.


## Exporting your data

    tapsoob pull [OPTIONS] <dump_path> <database_url>

The `dump_path` is the location on the filesystem where you want your database exported and the `database_url` is an URL looking like this `postgres://user:password@localhost/blog`, you can find out more about how to connect to your RDBMS by refering yourself to this page: <http://sequel.rubyforge.org/rdoc/files/doc/opening_databases_rdoc.html> on the Sequel documentation.

Regarding options a complete list of options can be displayed using the following command:

    tapsoob pull -h


## Importing your data

    tapsoob push [OPTIONS] <dump_path> <database_url>

As for exporting the `dump_path` is the path you previously exported a database you now wish to import and the `database_url` should conform to the same format described before.

You can list all available options using the command:

    tapsoob push -h


## Notes

Your exports can be moved from one machine to another for backups or replication, you can also use Tapsoob to switch your RDBMS from one of the supported system to another.


## Feature(s) to come

* Tests
* Integration with Rails (as a Rake task)


## License

The MIT License (MIT)
Copyright © 2013 Félix Bellanger <felix.bellanger@gmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.