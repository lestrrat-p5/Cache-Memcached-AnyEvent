use inc::Module::Install;

name 'Cache-Memcached-AnyEvent';
all_from 'lib/Cache/Memcached/AnyEvent.pm';
requires 'AnyEvent';
requires 'AnyEvent::Socket';
requires 'AnyEvent::Handle';

requires 'String::CRC32'; # sorry, required

WriteAll;