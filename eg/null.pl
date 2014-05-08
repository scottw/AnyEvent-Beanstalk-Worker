#!/usr/bin/env perl
use strict;
use warnings;
use feature 'say';

use blib;
use AnyEvent::Beanstalk::Worker;
use AnyEvent;
use JSON;
use EV;

my @timer = ();

my $w = AnyEvent::Beanstalk::Worker->new
  ( max_jobs => 10000,
    concurrency => 10000,
    beanstalk_watch   => 'test',
    beanstalk_decoder => sub { eval { decode_json(shift) } }
  );

$w->on(
    reserved => sub {
        my $self = shift;
        my $job  = shift;

        print STDERR "job " . $job->id . " reserved\r";
        $self->finish(delete => $job->id);
    }
);

$w->start;

EV::run;

print STDERR "\n";
