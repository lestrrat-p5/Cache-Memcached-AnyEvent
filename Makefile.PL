use inc::Module::Install;

name 'Cache-Memcached-AnyEvent';
all_from 'lib/Cache/Memcached/AnyEvent.pm';
requires 'AnyEvent';
requires 'AnyEvent::Socket';
requires 'AnyEvent::Handle';

requires 'String::CRC32'; # sorry, required
requires 'Storable';

test_requires 'Test::More' => '0.94';
test_requires 'Test::Memcached';

recommends 'Compress::Zlib';

readme_from 'lib/Cache/Memcached/AnyEvent.pm';
auto_set_repository;

WriteAll;
