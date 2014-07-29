use 5.008002;

use FindBin;
use ExtUtils::MakeMaker;

WriteMakefile(
	AUTHOR         => 'Alexander Borisov <lex.borisov@gmail.com>',
	ABSTRACT       => 'YAUID.pm',
    VERSION_FROM   => 'YAUID.pm',
    NAME           => 'Number::YAUID',
	INC            => '-I'. $FindBin::Bin ."/yauid",
);
