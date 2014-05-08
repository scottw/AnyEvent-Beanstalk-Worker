package AnyEvent::Beanstalk::Worker;

use 5.016001;
use strict;
use warnings;
use feature 'current_sub';
use AnyEvent;
use AnyEvent::Log;
use AnyEvent::Beanstalk;

our $VERSION = '0.01';

sub new {
    my $class = shift;
    my $self  = {};
    bless $self => $class;

    my %args = @_;

    $self->{_cb}     = {};
    $self->{_event}  = {};
    $self->{_jobs}   = {};
    $self->{_events} = [];    ## event queue
    $self->{_handled_jobs} = 0;  ## simple job counter

    $self->{_running}        = 0;
    $self->{_stop_tries}     = 0;
    $self->{_max_stop_tries} = $args{max_stop_tries} // 3;
    $self->{_max_jobs}       = $args{max_jobs} || 0;
    $self->{_concurrency}    = $args{concurrency} || 1;
    $self->{_log_level}      = $args{log_level} // 4;

    $self->{_reserve_timeout}        = $args{reserve_timeout} || 1;
    $self->{_reserve_base}           = $self->{_reserve_timeout};
    $self->{_reserve_timeout_factor} = 1.1;
    $self->{_reserve_timeout_max}    = 4;
    $self->{_release_delay}          = $args{release_delay} || 3;

    $self->{_log_ctx} = AnyEvent::Log::ctx;
    $self->{_log_ctx}->title(__PACKAGE__);

    $self->{_log}          = {};
    $self->{_log}->{trace} = $self->{_log_ctx}->logger("trace");
    $self->{_log}->{debug} = $self->{_log_ctx}->logger("debug");
    $self->{_log}->{info}  = $self->{_log_ctx}->logger("info");
    $self->{_log}->{note}  = $self->{_log_ctx}->logger("note");

    $self->{_signal} = {};
    $self->{_signal}->{TERM} = AnyEvent->signal(
        signal => "TERM",
        cb =>
          sub { $self->{_log_ctx}->log( warn => "TERM received" ); $self->stop }
    );
    $self->{_signal}->{INT} = AnyEvent->signal(
        signal => "INT",
        cb =>
          sub { $self->{_log_ctx}->log( warn => "INT received" ); $self->stop }
    );
    $self->{_signal}->{USR2} = AnyEvent->signal(
        signal => "USR2",
        cb     => sub {
            $self->{_log_level} =
              ( $self->{_log_level} >= 9 ? 2 : $self->{_log_level} + 1 );
        }
    );

    $args{beanstalk_host} ||= 'localhost';
    $args{beanstalk_port} ||= 11300;

    $self->beanstalk(
        server  => $args{beanstalk_host} . ':' . $args{beanstalk_port},
        decoder => $args{beanstalk_decoder}
    );

    $self->beanstalk->watch( $args{beanstalk_watch} )->recv;

    $self->on(
        start => sub {
            my $self = shift;
            my $reason = shift || '(unknown)';

            $self->{_log}->{trace}->("in start: $reason");

            unless ( $self->{_running} ) {
                $self->{_log}->{trace}->("worker is not running");
                return;
            }

            unless ( $self->job_count < $self->concurrency ) {
                $self->{_log}->{trace}->( "worker running "
                      . $self->job_count
                      . " jobs; will not accept more jobs until others finish"
                );
                return;
            }

            if ( $self->max_jobs and $self->handled_jobs >= $self->max_jobs ) {
                $self->{_log}->{info}->("Handled " . $self->handled_jobs . "; will not accept more jobs");
                return;
            }

            if ( ! $self->job_count and $self->{_stop_tries} ) {
                $self->{_log}->{info}->("No jobs left; stopping as requested");
                return $self->stop;
            }

            $self->beanstalk->reserve(
                $self->{_reserve_timeout},
                sub {
                    my ( $qjob, $qresp ) = @_;
                    $qresp //= '';

                    if ( $qresp =~ /timed_out/i ) {
                        $self->{_reserve_timeout} *=
                          $self->{_reserve_timeout_factor}
                          unless $self->{_reserve_timeout} >=
                          $self->{_reserve_timeout_max};
                        $self->{_log}->{trace}
                          ->("beanstalk reservation timed out");
                        return $self->emit( start => ($qresp) );
                    }

                    unless ( $qresp =~ /reserved/i ) {
                        $self->{_log}->{note}->("beanstalk returned: $qresp")
                          unless $qresp =~ /deadline_soon/i;
                        return $self->emit( start => ($qresp) );
                    }

                    $self->{_reserve_timeout} = $self->{_reserve_base};

                    if ( $self->{_jobs}->{ $qjob->id } ) {
                        $self->{_log_ctx}->log( warn => "Already have "
                              . $qjob->id
                              . " reserved (must have expired)" );
                        return $self->emit( start => ("already reserved") );
                    }

                    $self->{_jobs}->{ $qjob->id } = 1;
                    $self->{_handled_jobs}++;

                    $self->{_log}->{info}->( "added job "
                          . $qjob->id
                          . "; outstanding jobs: "
                          . $self->job_count );

                    $self->{_log}->{trace}->( "reserved job " . $qjob->id );
                    $self->emit( reserved => @_ );
                    $self->emit( start    => ('reserved') );
                }
            );
        }
    );

    ## FIXME: thinking about when to touch jobs, how to respond to
    ## FIXME: NOT_FOUND, etc. after timeouts

    ## FIXME: think about logging for clarity; figure out how to
    ## FIXME: filter 'note' level messages (for example)

    $self->on(
        reserved => sub {
            my $self = shift;
            my ( $qjob, $qresp ) = @_;

            $self->{_log}->{debug}->( "reserved a job: " . $qjob->id );
            $self->finish(
                release => $qjob->id,
                { delay => $self->{_release_delay} }
            );
        }
    );

    $self->init(@_);

    return $self;
}

sub init { }

sub start {
    my $self = shift;
    $self->{_running}    = 1;
    $self->{_stop_tries} = 0;
    $self->{_log}->{trace}->("starting worker");
    $self->emit( start => ('start sub') );
}

sub finish {
    my $self   = shift;
    my $action = shift;
    my $job_id = shift;
    my $cb     = pop;
    my $args   = shift;

    ## FIXME: find a clean way to execute our code *and* the callback
    if ( ref($cb) ne 'CODE' ) {
        $args = $cb;
        $cb = sub { };
    }

    my $internal = sub {
        delete $self->{_jobs}->{$job_id};    ## IMPORTANT

        if ( $self->job_count == $self->concurrency - 1 ) {
            ## we've been waiting for a slot to free up
            $self->emit( start => ('finish sub') );
        }

        $self->{_log}->{info}
          ->( "finished with $job_id ($action); outstanding jobs: "
              . $self->job_count );

#        $cb->($job_id);

        ## we're done
        if ( $self->max_jobs
             and $self->handled_jobs >= $self->max_jobs
             and ! $self->job_count ) {
            $self->{_log}->{info}->("Handled " . $self->handled_jobs . "; quitting");
            return $self->stop;
        }
    };

    eval {
        $self->beanstalk->$action( $job_id, ( $args ? $args : () ), $internal );
    };

    $self->{_log_ctx}->log(
        error => "first argument to finish() must be a beanstalk command: $@" )
      if $@;
}

sub stop {
    my $self = shift;
    $self->{_stop_tries}++;

    if ( $self->{_stop_tries} >= $self->{_max_stop_tries} ) {
        $self->{_log_ctx}->log(
            warn => "stop requested; impatiently quitting outstanding jobs" );
        exit;
    }

    if ( $self->job_count ) {
        $self->{_log_ctx}
          ->log( warn => "stop requested; waiting for outstanding jobs" );
        return;
    }

    $self->{_log_ctx}->log( fatal => "exiting" );
    exit;
}

sub on {
    my ( $self, $event, $cb ) = @_;

    $self->{_cb}->{$event} = $cb;

    $self->{_event}->{$event} = sub {
        my $evt = shift;
        AnyEvent->condvar(
            cb => sub {
                if ( ref( $self->{_cb}->{$evt} ) eq 'CODE' ) {
                    $self->{_log}->{trace}->("event: $evt");
                    my @data = $_[0]->recv;
                    $self->{_log}->{debug}->(
                        "shift event ($evt): " . shift @{ $self->{_events} } );
                    $self->{_log}->{debug}->(
                        "EVENTS (s): " . join( ' ' => @{ $self->{_events} } ) );
                    $self->{_cb}->{$evt}->(@data);
                }

                $self->{_event}->{$evt} = AnyEvent->condvar( cb => __SUB__ );
            }
        );
      }
      ->($event);
}

sub emit {
    my $self  = shift;
    my $event = shift;
    $self->{_log}->{debug}->("push event ($event)");
    push @{ $self->{_events} }, $event;
    $self->{_log}->{debug}
      ->( "EVENTS (p): " . join( ' ' => @{ $self->{_events} } ) );
    $self->{_event}->{$event}->send( $self, @_ );
}

sub beanstalk {
    my $self = shift;
    $self->{_beanstalk} = AnyEvent::Beanstalk->new(@_) if @_;
    return $self->{_beanstalk};
}

sub job_count { scalar keys %{ $_[0]->{_jobs} } }

sub handled_jobs { $_[0]->{_handled_jobs} }

sub max_jobs { $_[0]->{_max_jobs} }

sub concurrency {
    my $self = shift;

    if (@_) {
        $self->{_concurrency} = shift;
    }
    return $self->{_concurrency};
}

1;
__END__

=head1 NAME

AnyEvent::Beanstalk::Worker - Event-driven FSA for beanstalk queues

=head1 SYNOPSIS

  use AnyEvent::Beanstalk::Worker;
  use Data::Dumper;
  use JSON;
  use EV;

  my $w = AnyEvent::Beanstalk::Worker->new(
      concurrency       => 10,
      beanstalk_watch   => 'jobs',
      beanstalk_decoder => sub {
          eval { decode_json(shift) };
      }
  );

  $w->on(reserved => sub {
      my $self = shift;
      my ($qjob, $qresp) = @_;

      say "Got a job: " . Dumper($qjob->decode);

      shift->emit( my_next_state => $qjob );
  });

  $w->on(my_next_state => sub {
      my $self = shift;
      my $job  = shift;

      ## do something with job
      ...

      ## maybe not ready yet?
      unless ($job_is_ready) {
          return $self->finish(release => $job->id, { delay => 60 });
      }

      ## all done!
      $self->finish(delete => $job->id);
  });

  $w->start;
  EV::run;

=head1 DESCRIPTION

B<AnyEvent::Beanstalk::Worker> implements a simple, abstract
finite-state automaton for beanstalk queues. It can handle a
configurable number of concurrent jobs, and implements graceful worker
shutdown.

You are encouraged to subclass B<AnyEvent::Beanstalk::Worker> and
implement your own B<init> function, for example, so your object has
access to anything you need in subsequent states.

=head2 Caveat

This module represents the current results of an ongoing experiment
involving queues (beanstalk, AnyEvent::Beanstalk), non-blocking and
asynchronous events (AnyEvent), and state machines as means of
avoiding continuation-passing style programming (which is what you
typically get with callback-driven programs).

=head2 Introduction to beanstalk

B<beanstalkd> is a small, fast work queue written in C. When you need
to do lots of jobs (work units--call them what you will), such as
sending an email, fetching and parsing a web page, image processing,
etc.), a I<producer> (a small worker that creates jobs) adds jobs to
the queue. I<Consumer> workers come along and ask for jobs from the
queue, and then work on them. When the consumer is done, it deletes
the job from the queue and asks for another job.

=head2 Introduction to AnyEvent

B<AnyEvent> is an elegantly designed, generic interface to a variety
of event loops.

=head2 Introduction to event loops

I couldn't find any gentle introductions into event loops; I was going
to write one myself but realized it would probably turn into a
book. Additionally, I'm not qualified to write said book. With that
disclaimer, here is a brief, "close enough" introduction to event
loops which may help some people get an approximate mental model, good
enough to begin event programming.

An event loop is made up of two lists: event sources (sometimes just
"events") and event watchers (aka "listeners" or "observers"). Before
the loop starts, it (chooses its sources?)

FIXME

=head1 SEE ALSO

B<beanstalkd>, by Keith Rarick: L<http://kr.github.io/beanstalkd/>

B<AnyEvent::Beanstalk>, by Graham Barr: L<AnyEvent::Beanstalk>

B<AnyEvent>, by Marc Lehmann: L<http://anyevent.schmorp.de>

=head1 AUTHOR

Scott Wiersdorf, E<lt>scott@perlcode.orgE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2014 by Scott Wiersdorf

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.16.1 or,
at your option, any later version of Perl 5 you may have available.

=cut
