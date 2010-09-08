use inc::Module::Install;

name 'Cache-Memcached-AnyEvent';
all_from 'lib/Cache/Memcached/AnyEvent.pm';
requires 'AnyEvent';
requires 'AnyEvent::Socket';
requires 'AnyEvent::Handle';

requires 'String::CRC32'; # sorry, required
requires 'Storable';

test_requires 'Test::Exception';
test_requires 'Test::More' => '0.94';
test_requires 'Test::Memcached' => '0.00003';
test_requires 'Test::Requires';

recommends 'Compress::Zlib';

readme_from 'lib/Cache/Memcached/AnyEvent.pm';
auto_set_repository;
author_tests 'xt';

WriteAll;
