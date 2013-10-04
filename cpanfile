requires 'AnyEvent';
requires 'AnyEvent::Handle';
requires 'AnyEvent::Socket';
requires 'Class::Accessor::Lite';
requires 'Storable';
requires 'String::CRC32';
requires 'Task::Weaken';
recommends 'Compress::Zlib';
requires 'Module::Runtime';

on build => sub {
    requires 'ExtUtils::MakeMaker', '6.36';
    requires 'Test::Fatal';
    requires 'Test::Memcached', '0.00003';
    requires 'Test::More', '0.94';
    requires 'Test::Requires';
};
