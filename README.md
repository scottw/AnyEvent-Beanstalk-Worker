# AnyEvent::Beanstalk::Worker #

This module implements an event-driven finite state automata for
beanstalk queues. It ties together several interesting technologies:

* Beanstalk

A fast, generic in-memory job queue.

* AnyEvent::Beanstalk

An AnyEvent-based beanstalk client.

* AnyEvent

The DBI of event loop programming.

* FSA

Finite-state automata can model many simple event-driven programming
projects and avoid continuation-passing style.

## The problem ##

Event-driven programming can become unwieldy fast. Problems that
appear to have a simple callback solution:

    $ua->get($url => sub {
        say pop->res->body;
    });

quickly fall into an impenetrable style when we need to add another
callback or two inside an existing callback:

    $ua->get($url => sub {
        my ($ua, $tx) = @_;

        $tx->res->dom->find('a[href]')->each(sub {
            $ua->get(shift->{href}, sub {
                my ($ua, $tx) = @_;

                ...
            });
        });
    });

## About event loops ##

Feel free to skip this section if you're familiar with event loops
already.

FIXME

## Putting it all together ##

This module allows you to create a non-blocking beanstalk client
without the callback hell. Instead of adding a continuation, you
simply emit an event:

    $self->fetch($url);

which looks like this:

    $ua->get($url => sub {
        $self->emit(response => @_);
    });

And in the response state:

    my ($ua, $tx) = @_;

    $tx->res->dom->find('a[href]')->each(sub {
        $self->emit(fetch => shift->{href});
    });

The `emit` function adds the `fetch` event to the event queue, which
will be handled whenever the event loop comes around to it. In the
meantime, the event loop is handling everything else.

The benefits are clear. Besides the high concurrency you get with
non-blocking programming in general, using a FSA-style also gives you:

* modular, easy to follow code

* all events flow through a single point
