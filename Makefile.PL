use inc::Module::Install;

BEGIN {
    my @devmods = qw(
        Module::Install::AuthorTests
        Module::Install::ReadmeFromPod
        Module::Install::Repository
    );
    my @not_available;

    eval qq{use inc::Module::Install; 1;} or push @not_available, 'inc::Module::Install';
    for my $mod (@devmods) {
        eval qq{require $mod} or push @not_available, $mod;
    }
    if (@not_available) {
        print qq{# The following modules are not available.\n};
        print qq{# `$^X $0 | cpanm` will install them:\n};
        print $_, "\n" for @not_available;
        print "\n";
        exit -1;
    }
}

name 'Cache-Memcached-AnyEvent';
all_from 'lib/Cache/Memcached/AnyEvent.pm';
requires 'AnyEvent';
requires 'AnyEvent::Socket';
requires 'AnyEvent::Handle';
requires 'Task::Weaken';
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
