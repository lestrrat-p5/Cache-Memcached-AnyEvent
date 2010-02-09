use inc::Module::Install;

name 'Cache-Memcached-AnyEvent';
requires 'AnyEvent';
requires 'AnyEvent::Socket';
requires 'AnyEvent::Handle';

requires 'String::CRC32'; # sorry, required

WriteAll;