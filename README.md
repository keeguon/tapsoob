# Tapsoob

Tapsoob is a simple tool to export and import databases. It is inspired by <https://github.com/ricardochimal/taps> but instead of having to rely on a server and a client, databases are exported to the filesystem or imported from a previous export, hence the OOB (out-of-band). This was done in order to avoid exposing a whole database using the HyperText Protocol for security reasons.

Although the code is not quite perfect yet and some improvement will probably be made in the future it's usable. However if you notice an issue don't hesitate to report it.


## Database support

Tapsoob currently rely on the Sequel ORM (<http://sequel.rubyforge.org/>) so we currently support every database that Sequel supports. If additionnal support is required in the future (NoSQL databases) I'll make my best to figure out a way to bring that in my ToDo list.

### Oracle

If you're using either Oracle or Oracle XE you will need some extra requirements. If you're using Ruby you'll need to have your ORACLE_HOME environnement variable set properly and the `ruby-oci8` gem installed. However if you're using jRuby you'll need to have the official Oracle JDBC driver (see here for more informations: <http://www.oracle.com/technetwork/articles/dsl/jruby-oracle11g-330825.html>) and it should be loaded prior to using Tapsoob otherwise you won't be able to connect the database.


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


## NEW : Piping your schema/indexes/data

Due to some needs we added ways to pipe your schema/indexes/data directly from one database to another, here's an equivalent of the export/import process described above using this technique :

```
tapsoob schema dump <db_source_url> | tapsoob schema load <db_target_url> --drop=true
tapsoob schema indexes <db_source_url> | tapsoob schema load_indexes <db_target_url>
tapsoob data pull <db_source_url> -p false | tapsoob data push <db_target_url>
tapsoob schema reset_db_sequences <db_target_url>
```

A few notes here :

* the `--drop` option for the `schema load` command defaults to false, if you don't intend to drop your tables on your target databases you can ommit it (if a table already exists tapsoob will exit w/ an error) ;
* the `data pull` and `data push` commands are new and have a few options that you can display w/ `tapsoob data <pull|push> -h`, by defaults it prints the progress of your data extraction/import but if you want to pipe directly your export into another database you'll have to set the `--progress` (shorthand `-p`) option to `false` (or `--no-progress`) as above ;
* resetting database sequences is important as a `data push` command doesn't handle that directly.

## Integration with Rails

If you're using Rails, there's also two Rake tasks provided:

* `tapsoob:pull` which dumps the database into a new folder under the `db` folder
* `tapsoob:push` which reads the last dump you made from `tapsoob:pull` from the `db` folder


## Notes

Your exports can be moved from one machine to another for backups or replication, you can also use Tapsoob to switch your RDBMS from one of the supported system to another.


## ToDo

* Add a compression layer
* Tests (in progress)


## Contributors

* Félix Bellanger <felix.bellanger@gmail.com>
* Michael Chrisco <michaelachrisco@gmail.com>


## License

The MIT License (MIT)
Copyright © 2015 Félix Bellanger <felix.bellanger@gmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
