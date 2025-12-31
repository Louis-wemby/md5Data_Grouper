# md5Data_Grouper
An automated workflow for collecting, calculating (md5) and grouping large-scale genomic sequencing data.

## Please notice
When using the ``fastmd5`` command, the results can vary depending on the value of ``-s`` parameter. When we set ``-s`` to:
- 0: the output is MD5 value, a string with **32** characters (by hex). Equal to ``md5sum``.
- 1 - 9: the output is a string with **66** characters, which is the connection of SHA256 value and two charcters, s1/s2/.../s9, representing ``-s`` parameters since the SHA256 values under different speed levels differ from each other.
